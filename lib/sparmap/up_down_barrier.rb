module Sparmap
  # A simple barrier which admits both increments and decrements. Threads calling {#wait} will be blocked as long
  # the barrier {#count} is greater than zero, and will be release when it becomes zero.
  class UpDownBarrier

    # The current value for this {UpDownBarrier}'s counter.
    attr_reader :count

    # Creates a new {UpDownBarrier} with an initial count of `count`.
    #
    # @param initial_count [Integer] the initial value for the {UpDownBarrier} counter.
    def initialize(initial_count = 0)
      @mutex = Mutex.new
      @parked_threads = Array.new
      @count = initial_count
    end

    # Increments the {UpDownBarrier} counter by 1.
    def count_up
      @mutex.synchronize { @count += 1 }
    end

    # Decrements the {UpDownBarrier} counter by 1.
    def count_down
      @mutex.synchronize do
        @count = [@count - 1, 0].max
        unpark_all if @count == 0
      end
    end

    # Blocks the current thread until the this barrier's {#count} is zero, if it is
    # currently larger than zero, or returns immediately otherwise.
    def wait
      parking_spot = nil
      @mutex.synchronize do
        return if @count == 0
        parking_spot = Concurrent::Semaphore.new(0)
        @parked_threads << parking_spot
      end
      parking_spot.acquire
    end

    private

    def unpark_all
      @parked_threads.each { |parking_spot| parking_spot.release }
      @parked_threads.clear
    end
  end
end