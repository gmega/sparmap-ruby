require 'ostruct'

# {Sparmap::Pool} implements a thread-based worker pool which can run tasks in parallel through one of its task
# submission methods. Currently, the only submission method available is {#imap_unordered}. The API is inspired by
# the {https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool Python multiprocessing API}.

module Sparmap
  class Pool

    POISON_PILL = Object.new
    POLLING_INTERVAL = 0.1

    private_constant :POISON_PILL
    private_constant :POLLING_INTERVAL

    # Creates a new worker {Pool}.
    #
    # @param workers [Integer] the number of worker threads that this {Pool} should spawn. Worker threads are spawned
    #   immediately upon {Pool} creation, and are not destroyed until {#shutdown} is called or the program exists.
    #
    # @param queue_length [Integer] the size of the task queue. The task queue is used internally to buffer tasks from
    #   the input enumerable. A smaller queue length trades off memory consumption for higher potential latency should
    #   the queue stall workers by becoming empty. If unsure, leave it as it at its default value.
    def initialize(workers: 5, queue_length: workers * 3, &block)
      @n_workers = workers

      @work_queue = SizedQueue.new(queue_length)
      #noinspection RubyArgCount
      @shutdown_latch = Concurrent::CountDownLatch.new(workers)
      #noinspection RubyArgCount
      @running = Concurrent::AtomicBoolean.new(true)
      #noinspection RubyArgCount
      @active_jobs = Sparmap::UpDownBarrier.new

      @job_counter = 0
      @job_cache = Concurrent::Hash.new

      @worker_pool = (1..workers).each do |worker_id|
        thread = Thread.new { worker_loop(worker_id) }
        thread.name = "worker #{worker_id}"
      end
    end

    # Similarly to `Enumerable#map`, {#imap_unordered} returns an array with the results of running
    # `block` once for each element in the input `enumerable`. Unlike `Enumerable#map`, however,
    # {#imap_unordered} runs these operations in parallel, leveraging the available workers in this
    # worker {Pool}.
    #
    # @param enumerable [Enumerable] an `Enumerable` containing the data to be processed in parallel
    # @yield a code block which will be called, in parallel, for each element in the input enumerable
    #
    # @raise [StandardError] if this {Pool} has been shutdown with {#shutdown}
    #
    # @return [Enumerable] a lazy `Enumerable`, which will be populated with results as those are made available.
    #   The `Enumerable` will block the caller thread if no results are available. {#imap_unordered} does not preserve
    #   the order of the input `enumerable`, with results that get processed first being returned first.
    def imap_unordered(enumerable, &block)
      raise 'Pool has been shut down' unless running?

      job_id = @job_counter += 1

      # We don't allow the output queue to be resized as we expect clients to consume results quickly.
      #noinspection RubyArgCount
      iterator = ConsumerIterator.new(@n_workers * 5, @job_cache, @active_jobs, job_id)
      #noinspection RubyArgCount
      job = OpenStruct.new(consumer: iterator, job_id: job_id, tasks_in_flight: Concurrent::AtomicFixnum.new(0))

      @job_cache[job_id] = job
      async_push(enumerable, block, job)
      @active_jobs.count_up
      iterator
    end

    # Shuts down the current {Pool}, terminating all worker threads. Tasks that are already running will
    # be allowed to complete. Tasks that are enqueued but not yet running may or may not be given a chance to run
    # and complete.
    def shutdown(synchronous = false)
      # This will:
      #   1) stop new jobs from being created
      #   2) stop workers from trying to acquire new tasks from the work queue
      #   3) stop pushers from pushing new tasks into the task queue
      @running.value = false

      # We then shutdown the consumers. This will cause clients to be notified, on the one hand,
      # and will let workers run to completion without blocking, on the other.
      @job_cache.values.each { |consumer| consumer.shutdown }

      # Finally, to stop pushers we must clear the work queue so that blocked pushers
      # get a chance to realize we're shutting down. Note this may deadlock if we have
      # more pushers than we have queue slots.
      #
      # FIXME this is probably the flimsiest part of the shutdown.
      @work_queue.clear

      if synchronous
        @shutdown_latch.wait
      end
    end

    # Is this {Pool} running? Only {Pool}s that are running can accept and execute tasks.
    #
    # @return `true` if the {Pool} is running, `false` otherwise. {Pool}s that are not running will not accept new
    #   tasks.
    def running?
      @running.value
    end

    # Waits, with somewhat lax guarantees, for the jobs that are currently running in
    # this {Pool} to terminate. This is actually only **guaranteed** to work if we're
    # sure that no other threads are submitting new tasks, which is the typical use case for
    # {Pool}.
    def wait_for_quiescence
      # Already quiescent.
      return if @job_cache.empty?

      # Otherwise waits for quiescence.
      @active_jobs.wait
    end

    # Convenience method for running a code block which requires a {Pool} with automatic shutdown at the end.
    #
    # ```ruby
    # result = Pool.with_pool(workers: 10) do |pool|
    #   pool.imap_unordered(...) { ... }
    # end
    # ```
    def self.with_pool(**args, &block)
      pool = Sparmap::Pool.new(**args)
      block.call(pool)
    ensure
      pool.shutdown(synchronous: true)
    end

    private

    def worker_loop(worker_id)
      storage = OpenStruct.new(storage: {}, worker_id: worker_id)

      while running?
        # Acquires work, non-blocking. The reason we can't block
        # is that otherwise shutdown becomes very difficult to implement since
        # there's no way to ensure we wake all workers from blocking.
        begin
          task = @work_queue.pop(non_block: true)
        rescue ThreadError
          sleep(POLLING_INTERVAL)
          next
        end

        # If no longer running, stops.
        break unless running?

        job = @job_cache[task.job_id]

        # Poison pill gets passed forward to consumer.
        if task.item == POISON_PILL
          job.consumer.output(POISON_PILL)
          next
        end

        # Processes task.
        begin
          result = storage.instance_exec(task.item, &task.block)
        rescue StandardError => error
          result = error
        end

        job.consumer.output(result)
      end
    ensure
      @shutdown_latch.count_down
    end

    def async_push(sequence, block, job)
      Thread.new do
        begin
          sequence.each do |item|
            job.tasks_in_flight.increment
            @work_queue.push(OpenStruct.new(block: block, item: item, job_id: job.job_id))
            return unless running?
          end
        ensure
          @work_queue.push(OpenStruct.new(block: nil, item: POISON_PILL, job_id: job.job_id))
        end
      end
    end

    # {ConsumerIterator} is the object through which clients can consume results from a parallel computation
    # started with {Pool#imap_unordered}. It implements both a Python-like {#next} method and the more standard
    # Ruby `Enumerable` interface.
    class ConsumerIterator
      include Enumerable

      # Creates a new {ConsumerIterator}. This is called by {Pool} and should not be used directly by
      # clients.
      def initialize(queue_length, job_cache, active_jobs, job_id)
        @output_queue = SizedQueue.new(queue_length)

        @job_cache = job_cache
        @job_id = job_id
        @active_jobs = active_jobs

        # Is the pusher for this job running?
        @pusher_done = false
        # Is the job done?
        @job_done = false
        # Is the enclosing Pool running?
        @running = true
      end

      # Returns the next result available from an {Pool#imap_unordered} operation, optionally blocking if no results
      # are currently available.
      #
      # This method should not be called by multiple threads at the same time.
      #
      # @param non_block [Boolean]: if set to true, blocks the caller if no results are available.
      #
      # @return [Object, nil] the next result available. If there are no results yet available, will return `nil`
      #   if `non_block` is set to `true` (default), or will block the caller otherwise.
      #
      # @raise [StopIteration] when there are no more results to consume.
      def next(non_block: true)
        raise StopIteration.new if @job_done || !@running

        # If there are no more tasks in flight and no more tasks being produced (pusher done),
        # then the job is done.
        if job.tasks_in_flight.value == 0 and @pusher_done
          @job_cache.delete(@job_id)
          @active_jobs.count_down
          @job_done = true
          raise StopIteration.new
        end

        # This will block the caller thread if nothing is available.
        result = @output_queue.pop(non_block)

        # We get a poison pill when the pusher is done pushing tasks. We may still
        # have tasks in flight at the workers, so we have to wait for those to be processed
        # before halting.
        if result == POISON_PILL
          @pusher_done = true
          # tail call
          return self.next(non_block: non_block)
        else
          job.tasks_in_flight.decrement
          return result
        end
      rescue ThreadError
        nil
      end

      # @return [Boolean] `false` if this {ConsumerIterator} has no more elements to return, or `true otherwise`.
      def done?
        @job_done
      end

      # Same as Enumerable#each
      def each(&block)
        begin
          loop { block.call(self.next(non_block: false)) }
        rescue StopIteration
          # Done.
        end
      end

      # @api private
      def shutdown
        @running = false
        # Shutdown requires the consumer to unblock, if blocked on a SizedQueue.pop, so that it
        # can see that the pool has shut down and notify that to clients.
        # We therefore try to push a single POISON_PILL. If we succeed, we're sure the consumer will
        # unblock. If we fail, it means the queue is full, and we're also sure that the consumer will
        # unblock.
        begin
          @output_queue.push(POISON_PILL, non_block: true)
        rescue ThreadError
          # We're good.
        end
      end

      # @api private
      def job
        @job_cache[@job_id]
      end

      # @api private
      def output(task_result)
        # If not running, returns immediately.
        return unless @running
        @output_queue.push(task_result)
      end

    end
  end
end