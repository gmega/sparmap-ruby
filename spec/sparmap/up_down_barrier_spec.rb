# frozen_string_literal: true

require 'spec_helper'

describe Sparmap::UpDownBarrier do
  it 'should block threads until zero' do
    n_threads = 10
    barrier = described_class.new(n_threads)

    observed = Concurrent::Array.new

    # We do a three phased test where we check that the counts
    # match what we expect at each. The expectation is that if
    # the barrier is not holding the threads, then the values will
    # not come out in the order we expect, especially given that
    # MRI Ruby threads will run undisturbed and happily prevent anyone
    # else from running, unless they block.
    phase1 = Concurrent::CountDownLatch.new(n_threads)
    phase2 = Concurrent::CountDownLatch.new(n_threads)
    phase3 = Concurrent::CountDownLatch.new(n_threads)

    threads = (0...n_threads).map do
      Thread.new do
        observed.append(barrier.count)
        phase1.count_down
        barrier.wait
        barrier.count_up
        observed.append(barrier.count)
        phase2.count_down
        barrier.wait
        observed.append(barrier.count)
        phase3.count_down
      end
    end

    phase1.wait
    expect(observed.length).to be(n_threads)
    expect(observed.map { |value| value == n_threads }.all?).to be(true)
    observed.clear

    (0...10).each { barrier.count_down }

    phase2.wait
    expect(observed.length).to be(n_threads)
    expect(observed[0...n_threads].map { |value| value > 0 }.all?).to be(true)
    observed.clear

    (0..10).each { barrier.count_down }
    phase3.wait

    expect(observed.length).to be(n_threads)
    expect(observed.map { |value| value == 0 }.all?).to be(true)
    
    threads.each { |t| t.join }
  end
end
