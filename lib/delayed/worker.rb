module Delayed
  class Worker
    SLEEP = 60

    cattr_accessor :logger
    self.logger = if defined?(::Rails::Logger)
      Rails.logger
    end

    def initialize(options={})
      @quiet = options[:quiet]
      Delayed::Job.min_priority = options[:min_priority] if options.has_key?(:min_priority)
      Delayed::Job.max_priority = options[:max_priority] if options.has_key?(:max_priority)
    end

    def start
      say "*** Starting job worker #{Delayed::Job.worker_name}"
      write_pid_file

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = Delayed::Job.work_off
        end

        count = result.sum

        break if $exit

        if count.zero?
          sleep(SLEEP)
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end

        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!
      remove_pid_file
    end

    def write_pid_file
      File.open(self.pid_file, 'w') do |pid_file|
        pid_file.write Process.pid
      end
    end

    def remove_pid_file
      File.unlink self.pid_file
    end

    def pid_file
      if defined?(::Rails.root)
        ::Rails.root.to_s + "/tmp/pids/delayed_job.#{Process.pid}.pid"
      else
        "./delayed_job.#{Process.pid}.pid"
      end
    end

    def say(text)
      puts text unless @quiet
      logger.info text if logger
    end

  end
end
