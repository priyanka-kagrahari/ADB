# site.py

from collections import defaultdict

class Site:
    def __init__(self, site_id):
        self.id = site_id
        self.status = "up"
        self.data = {}
        self.commit_history = {}
        self.recovery_time = None
        self.failure_history = []  # Times when the site failed
        self.recovery_history = []  # Times when the site recovered

    def fail(self, fail_time):
        self.status = "down"
        self.failure_history.append(fail_time)
        print(f"Site {self.id} fails at time {fail_time}")

    

    def initialize_data(self):
        """ Initialize the data for 20 variables x1 to x20 """
        for i in range(1, 21):  # Initialize data for 20 variables x1 to x20
            variable_name = f"x{i}"
            if self.id == 1 or i % 2 == 0 or (1 + i % 10) == self.id:
                # Initialize the variable if it belongs to the site based on your rules
                self.data[variable_name] = 10 * i
                self.commit_history[variable_name] = [(0, 10 * i)]  # Starting value is 10*i

    def was_up_continuously_between(self, start_time, end_time):
        # Build a list of down intervals
        down_intervals = []
        failure_times = self.failure_history[:]
        recovery_times = self.recovery_history[:]

        # If the site is currently down, add an open-ended down interval
        if self.status == 'down':
            failure_times.append(self.failure_history[-1])
            recovery_times.append(float('inf'))

        for fail_time, recover_time in zip(failure_times, recovery_times):
            down_intervals.append((fail_time, recover_time))

        # Check for any overlap with the interval [start_time, end_time)
        for down_start, down_end in down_intervals:
            if down_start < end_time and down_end > start_time:
                return False  # Site was down during the required interval

        return True


    def is_failed(self):
        """ Returns whether the site is in a failed state """
        return self.status == "down"


    def get_last_commit_time(self, variable):
        if variable in self.commit_history and self.commit_history[variable]:
            return self.commit_history[variable][-1][0]  # Last commit time
        else:
            return 0  # If no commit history, return 0 (initial value time)

    def was_up_continuously_between(self, start_time, end_time):
        # If the site failed at any point between start_time and end_time, return False
        for i in range(len(self.failure_history)):
            fail_time = self.failure_history[i]
            # Determine the corresponding recovery time
            recover_time = self.recovery_history[i] if i < len(self.recovery_history) else float('inf')
            if fail_time < end_time and recover_time > start_time:
                # There is an overlap with the downtime and the required interval
                return False
        return True

    def get_last_commit_time(self, variable):
        if variable in self.commit_history and self.commit_history[variable]:
            return self.commit_history[variable][-1][0]  # Return the last commit time
        else:
            return 0  # If no commit history, return 0 (initial value time)



    def recover(self, current_time, committed_transactions):
        self.status = 'up'
        self.recovery_history.append(current_time)
        print(f"Site {self.id} recovers at time {current_time}")

        print(f"DEBUG: Site {self.id} starts recovery at time {current_time}")

        # Apply all committed writes after the failure
        for txn in committed_transactions:
            if txn.end_time > self.failure_history[-1] and txn.end_time <= current_time:
                print(f"DEBUG: Applying committed writes from Transaction {txn.id} to Site {self.id}")
                for variable, value in txn.get_committed_variables():
                    # Apply each committed write to the site
                    self.apply_committed_write(variable, value)

    def apply_committed_write(self, txn_timestamp, variable, value):
        """ Apply the committed write for a variable after recovery """
        if txn_timestamp > self.recovery_time:  # The txn timestamp should be after recovery time
            if variable not in self.commit_history:
                self.commit_history[variable] = [(txn_timestamp, value)]
            else:
                # Append to the commit history at the recovery point
                self.commit_history[variable].append((txn_timestamp, value))
            self.data[variable] = value
            print(f"DEBUG: Site {self.id} applies committed write: {variable} = {value}")

    def is_up(self):
        """ Return True if the site is up """
        return self.status == "up"

    def get_last_committed_value(self, variable, timestamp):
        """ Retrieve the last committed value of a variable at or before the given timestamp """
        if variable not in self.commit_history:
            return 10 * int(variable.strip().lstrip('x'))  # Return default initial value
        
        for commit_time, value in reversed(self.commit_history[variable]):
            if commit_time <= timestamp and value is not None:
                return value

        return 10 * int(variable.strip().lstrip('x'))  # Default value if no commit found

    def is_variable_readable(self, variable, timestamp):
        """ Determine if the variable is readable based on its replication status and site failure state """
        var_index = int(variable.strip().lstrip('x'))
        if var_index % 2 == 1:
            # Non-replicated variable: check if the site is up
            return self.is_up()
        else:
            # Replicated variable: check if the site is up and has committed value
            return self.is_up() and (self.recovery_time is None or 
                                     self.get_last_committed_value(variable, timestamp) is not None)

    def has_failed_since(self, timestamp):
        """ Returns True if the site failed after the given timestamp """
        return any(fail_time > timestamp for fail_time in self.failure_history)

    def mark_variable_unreadable(self, variable):
        """ Mark a variable as unreadable by appending a "None" entry at the recovery time """
        if variable in self.commit_history:
            self.commit_history[variable].append((self.recovery_time, None))
        else:
            self.commit_history[variable] = [(self.recovery_time, None)]

    def write(self, variable, value, timestamp):
        """ Write a value for the variable at the current time """
        if variable in self.commit_history:
            self.commit_history[variable].append((timestamp, value))
        else:
            self.commit_history[variable] = [(timestamp, value)]
        self.data[variable] = value
