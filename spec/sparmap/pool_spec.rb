# frozen_string_literal: true

require 'spec_helper'

describe Sparmap::Pool do
  it 'should run job correctly to termination' do
    input = (1...2000).to_a
    processing = -> (x) { x * 2 }

    expected_result = input.map(&processing)

    pool = Sparmap::Pool.new(workers: 20)
    result = pool.imap_unordered(input, &processing).to_a.sort

    expect(result).to eq expected_result

    pool.shutdown(synchronous: true)
  end

  it 'accepts multiple jobs' do
    # In this test, we run 10 jobs at the same time and check that mixing stuff in the
    # common worker pool does not mess up the results.
    n_jobs = 10
    n_workers = 20
    job_span = (1..2000)

    # Each job has its processing function.
    processing = (1..n_jobs).map { |factor| -> (x) { x * factor } }.to_a

    # To increase chances that job inputs get mixed, we randomly stall the start of each
    # input generator.
    stalled_inputs = (1..n_jobs).map do
      Enumerator.new { |y| sleep(rand(0.1..0.5)); job_span.each { |i| y.yield i } }
    end

    # Expected outputs computed sequentially.
    expected_outputs = (1..n_jobs).map { |job| job_span.map { |x| processing[job - 1].call(x) } }

    pool = Sparmap::Pool.new(workers: n_workers)
    iterators = stalled_inputs.each_with_index.map { |input, idx| pool.imap_unordered(input, &processing[idx]) }

    # We'll now consume from iterators as they have results available, in a non-blocking fashion.
    results = Hash.new { |h, k| h[k] = [] }
    done = 0
    while done != n_jobs
      # This keeps track as to whether we managed to find at least one iterator
      # ready at each scan.
      progress = false

      # Scans all iterators.
      iterators.each_with_index.filter { |iterator, _| !iterator.done? }.each do |iterator, idx|
        begin
          element = iterator.next
          unless element.nil?
            results[idx] << element
            progress = true
          end
        rescue StopIteration
          done += 1
          puts "#{idx}: #{results[idx].size}"
        end
      end

      # No progress, sleeps for a bit.
      unless progress
        sleep(0.5)
      end
    end

    # Check that results match.
    expected_outputs.each_with_index.each do |expected, idx|
      expect(expected).to eq(results[idx].sort)
    end

    pool.shutdown(synchronous: true)
  end

  it 'maintains a thread-local storage object in context' do
    pool = Sparmap::Pool.new(workers: 10)
    storing = pool.imap_unordered(1..10) { |i| sleep(0.5); storage['index'] = i; i }.to_a

    expect((1..10).to_a).to eq(storing.sort)

    retrieving = pool.imap_unordered(1..10) { sleep(0.5); storage['index'] }.to_a

    expect((1..10).to_a).to eq(retrieving.sort)

    pool.shutdown(synchronous: true)
  end

  it 'shuts down properly when used within with_pool blocks' do
    result = nil
    pool_ref = nil
    Sparmap::Pool.with_pool(workers: 10) do |pool|
      result = pool.imap_unordered(0..20) do |i|
        sleep(rand(0...3.0))
        i
      end.to_a
      pool_ref = pool
    end

    expect(pool_ref.running?).to be(false)
    expect(result.sort).to eq((0..20).to_a)
  end
end
