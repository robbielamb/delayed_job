require 'dm-core'
require 'dm-timestamps'
require 'dm-types'

module Delayed

  class DeserializationError < StandardError
  end

  class Job
    include DataMapper::Resource
    MAX_ATTEMPTS = 25
    MAX_RUN_TIME = 14400
    
    storage_names[:default] = 'delayed_jobs'
    
    property   :id,         Serial
    property   :priority,   Integer,  :default => 0  # Allows some jobs to jump to the front of the queue
    property   :attempts,   Integer,  :default => 0  # Provides for retries, but still fail eventually.
    property   :handler,    Yaml                     # YAML-encoded string of the object that will do work
    property   :last_error, String                   # reason for last failure (See Note below)
    property   :run_at,     DateTime                 # When to run. Could be Time.now for immediately, or sometime in the future.
    property   :locked_at,  DateTime                 # Set when a client is working on this object
    property   :failed_at,  DateTime                 # Set when all retries have failed (actually, by default, the record is deleted instead)
    property   :locked_by,  String                   # Who is working on this object (if locked)
    
    timestamps :at
    
    before :save do
      self.run_at ||= Time.now
    end
    
    # By default failed jobs are destroyed after too many attempts.
    # If you want to keep them around (perhaps to inspect the reason
    # for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true
    
    # Every worker has a unique name which by default is the pid of the process.
    # There are some advantages to overriding this with something which survives worker retarts:
    # Workers can safely resume working on tasks which are locked by themselves. The worker will assume that it crashed before.
    cattr_accessor :worker_name
    self.worker_name = "host:#{Socket.gethostname} pid:#{Process.pid}" rescue "pid:#{Process.pid}"
    
    cattr_accessor :min_priority, :max_priority
    self.min_priority = nil
    self.max_priority = nil
    
    # When a worker is exiting, make sure we don't have any locked jobs.
    def self.clear_locks!
      # update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
      all(:locked_by => worker_name).update(:locked_by => nil, :locked_at => nil)
    end

    def failed?
      failed_at
    end
    alias_method :failed, :failed?
    
    alias_method :payload_object,  :handler
    alias_method :payload_object=, :handler=
    
    def name
      @name ||= begin
        payload = payload_object
        if payload.respond_to?(:display_name)
          payload.display_name
        else
          payload.class.name
        end
      end
    end
    
    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(message, backtrace = [], time = nil)
      if self.attempts < MAX_ATTEMPTS
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.last_error   = message + "\n" + backtrace.join("\n")
        self.unlock
        save!
      else
        ::Rails.logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
        destroy_failed_jobs ? destroy : update(:failed_at, Delayed::Job.db_time_now)
      end
    end
    
    # Try to run one job. Returns true/false (work done/work failed) or nil if job can't be locked.
    def run_with_lock(max_run_time, worker_name)
      ::Rails.logger.info "* [JOB] aquiring lock on #{name}"
      unless lock_exclusively!(max_run_time, worker_name)
        # We did not get the lock, some other worker process must have
        ::Rails.logger.warn "* [JOB] failed to aquire exclusive lock for #{name}"
        return nil # no work done
      end

      begin
        runtime =  Benchmark.realtime do
          invoke_job # TODO: raise error if takes longer than max_run_time
          destroy
        end
        # TODO: warn if runtime > max_run_time ?
        ::Rails.logger.info "* [JOB] #{name} completed after %.4f" % runtime
        return true  # did work
      rescue Exception => e
        reschedule e.message, e.backtrace
        log_exception(e)
        return false  # work failed
      end
    end
    
    # Add a job to the queue
    def self.enqueue(*args, &block)
      object = block_given? ? EvaledJob.new(&block) : args.shift

      unless object.respond_to?(:perform) || block_given?
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end
    
      priority = args.first || 0
      run_at   = args[1]

      Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
    end
    
    # Find a few candidate jobs to run (in case some immediately get locked by others).
    # Return in random order prevent everyone trying to do same head job at once.
    def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)

      # NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL'
      #  NextTaskOrder       = 'priority DESC, run_at ASC'
      # 
       time_now   = Time.now
       time_range = time_now - max_run_time
      # 
      #  sql = NextTaskSQL.dup
      # 
      #  conditions = [time_now, time_now - max_run_time, worker_name]
      # 
      #  if self.min_priority
      #    sql << ' AND (priority >= ?)'
      #    conditions << min_priority
      #  end
      # 
      #  if self.max_priority
      #    sql << ' AND (priority <= ?)'
      #    conditions << max_priority
      #  end
      # 
      #  conditions.unshift(sql)
      # 
      #  records = ActiveRecord::Base.silence do
      #    find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
      #  end

      records = self.all(:run_at.lte => time_now) & (self.all(:locked_at => nil) | self.all(:locked_at.lt => time_range )) | self.all(:locked_by => worker_name)
      records = records & self.all(:failed_at => nil)
      if self.min_priority
        records = records & self.all(:priority.gte => min_priority)
      end
      if self.max_priority
        records = records & self.all(:priority.lte => max_priority)
      end
      records = records.all(:order => [:priority.desc, :run_at.asc], :limit => limit)
      records.sort_by { rand() }
    end
    
    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def self.reserve_and_run_one_job(max_run_time = MAX_RUN_TIME)

      # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      find_available(5, max_run_time).each do |job|
        t = job.run_with_lock(max_run_time, worker_name)
        return t unless t == nil  # return if we did work (good or bad)
      end

      nil # we didn't do any work, all 5 were not lockable
    end
    
    # Lock this job for this worker.
     # Returns true if we have the lock, false otherwise.
     def lock_exclusively!(max_run_time, worker = worker_name)
       now = Time.now
       affected_rows = if locked_by != worker
         # We don't own this job so we will update the locked_by name and the locked_at
         # self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?)", id, (now - max_run_time.to_i)])
         klass = self.class
         (klass.all(:id => id) & (klass.all(:locked_at => nil) | klass.all(:locked_at => now))).update(:locked_at => now, :locked_by => worker)
       else
         # We already own this job, this may happen if the job queue crashes.
         # Simply resume and update the locked_at
         # self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
         self.class.all(:id => id, :worker => worker).update(:locked_at => worker)
       end
       if affected_rows == true
         self.locked_at    = now
         self.locked_by    = worker
         return true
       else
         return false
       end
     end
    
     # Unlock this job (note: not saved to DB)
     def unlock
       self.locked_at    = nil
       self.locked_by    = nil
     end
     
     # This is a good hook if you need to report job processing errors in additional or different ways
     def log_exception(error)
       ::Rails.logger.error "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{attempts} failed attempts"
       ::Rails.logger.error(error)
     end

     # Do num jobs and return stats on success/failure.
     # Exit early if interrupted.
     def self.work_off(num = 100)
       success, failure = 0, 0

       num.times do
         case self.reserve_and_run_one_job
         when true
             success += 1
         when false
             failure += 1
         else
           break  # leave if no work could be done
         end
         break if $exit # leave if we're exiting
       end

       return [success, failure]
     end

     # Moved into its own method so that new_relic can trace it.
     def invoke_job
       payload_object.perform
     end
     
     # Get the current time
     def self.db_time_now
       Time.now
     end
  end


  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
