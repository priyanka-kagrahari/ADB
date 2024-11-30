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
        """Simulate a failure of the site."""
        self.status = "down"
        self.failure_history.append(fail_time)
        print(f"Site {self.id} fails at time {fail_time}")

    def initialize_data(self):
        """ Initialize the data for 20 variables x1 to x20. """
        for i in range(1, 21):  # Variables x1 to x20
            variable_name = f"x{i}"
            if i % 2 == 0:
                # Even-indexed variables are replicated at all sites
                self.data[variable_name] = 10 * i
                self.commit_history[variable_name] = [(0, 10 * i)]
            else:
                # Odd-indexed variables are located at site 1 + (i % 10)
                site_id_for_variable = 1 + (i % 10)
                if self.id == site_id_for_variable:
                    self.data[variable_name] = 10 * i
                    self.commit_history[variable_name] = [(0, 10 * i)]

    def was_up_continuously_between(self, start_time, end_time):
        """Check if the site was up continuously between start_time and end_time."""
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
        """Returns whether the site is in a failed state."""
        return self.status == "down"

    def get_last_commit_time(self, variable):
        """Returns the last commit time for a variable."""
        if variable in self.commit_history and self.commit_history[variable]:
            return self.commit_history[variable][-1][0]  # Last commit time
        else:
            return 0  # If no commit history, return 0 (initial value time)
        
    def print_site_state(self, label):
        """Print the state of the site at the given point in time."""
        print(f"State of Site {self.id} at {label}:")
        for variable, value in sorted(self.data.items()):
            print(f"{variable}: {value}")
        print("\n")

    def recover(self, current_time, committed_transactions, sites_up):
        """Simulate the recovery of the site."""
        self.status = 'up'
        self.recovery_history.append(current_time)
        print(f"DEBUG: Site {self.id} recovers at time {current_time}")
        
        # Apply only those committed transactions that occurred before the failure time
        for txn in committed_transactions:
            txn_commit_time = txn.end_time
            site_failure_time = self.failure_history[-1] if self.failure_history else -1
            
            if txn_commit_time <= site_failure_time:
                # Apply writes only if the site was up when the transaction started
                if self.id in sites_up:
                    print(f"DEBUG: Site {self.id} applying writes from Transaction {txn.id} (commit time {txn_commit_time})")
                    for variable, value in txn.get_committed_variables():
                        print(f"DEBUG: Site {self.id} applying write: {variable} = {value} from Transaction {txn.id}")
                        self.apply_committed_write(txn_commit_time, variable, value)
                else:
                    print(f"DEBUG: Site {self.id} skips Transaction {txn.id}, it was down when the transaction started.")
            else:
                print(f"DEBUG: Skipping Transaction {txn.id} for Site {self.id}, txn commit time {txn_commit_time} is after site failure time")


    def apply_committed_write(self, txn_end_time, variable, value):
        """Apply the committed write for a variable after recovery."""
        print(f"DEBUG: Attempting to apply write for {variable} = {value} at txn_end_time: {txn_end_time}, site failure history: {self.failure_history}")

        # Check if transaction end time is before the failure time
        if txn_end_time <= self.failure_history[-1]:  # Only apply writes if txn was committed before failure
            if variable not in self.commit_history:
                self.commit_history[variable] = [(txn_end_time, value)]
            else:
                self.commit_history[variable].append((txn_end_time, value))

            self.data[variable] = value
            print(f"DEBUG: Site {self.id} applies committed write: {variable} = {value} (txn_end_time: {txn_end_time})")
        else:
            print(f"DEBUG: Write {variable} = {value} ignored for Site {self.id}, txn_end_time: {txn_end_time} after failure time")



    def mark_variable_unreadable(self, variable):
        """Mark a variable as unreadable by appending a "None" entry at the recovery time."""
        if variable in self.commit_history:
            self.commit_history[variable].append((self.recovery_time, None))
        else:
            self.commit_history[variable] = [(self.recovery_time, None)]




    def is_up(self):
        """Returns True if the site is up."""
        return self.status == "up"

    def get_last_committed_value(self, variable, timestamp):
        """Retrieve the last committed value of a variable at or before the given timestamp."""
        if variable not in self.commit_history:
            return 10 * int(variable.strip().lstrip('x'))  # Return default initial value
        
        for commit_time, value in reversed(self.commit_history[variable]):
            if commit_time <= timestamp and value is not None:
                return value

        return 10 * int(variable.strip().lstrip('x'))  # Default value if no commit found

    def is_variable_readable(self, variable, timestamp):
        """Determine if the variable is readable based on its replication status and site failure state."""
        var_index = int(variable.strip().lstrip('x'))
        if var_index % 2 == 1:
            # Non-replicated variable: check if the site is up
            return self.is_up()
        else:
            # Replicated variable: check if the site is up and has committed value
            return self.is_up() and (self.recovery_time is None or 
                                     self.get_last_committed_value(variable, timestamp) is not None)

    def has_failed_since(self, timestamp):
        """Returns True if the site failed after the given timestamp."""
        return any(fail_time > timestamp for fail_time in self.failure_history)

    def mark_variable_unreadable(self, variable):
        """Mark a variable as unreadable by appending a "None" entry at the recovery time."""
        if variable in self.commit_history:
            self.commit_history[variable].append((self.recovery_time, None))
        else:
            self.commit_history[variable] = [(self.recovery_time, None)]

    def write(self, variable, value, timestamp):
        """Write a value for the variable at the current time."""
        if variable in self.commit_history:
            self.commit_history[variable].append((timestamp, value))
        else:
            self.commit_history[variable] = [(timestamp, value)]
        self.data[variable] = value
