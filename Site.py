class Site:
    def __init__(self, site_id):
        self.id = site_id
        self.status = "up"  # Site starts as "up"
        self.data = {}  # Holds the data at the site
        self.commit_history = {}  # Holds the commit history for each variable
        self.recovery_time = None  # When the site will recover, None if not failed
        self.failure_history = []  # Tracks all failure times

    def initialize_data(self):
        for i in range(1, 21):  # Initialize data for 20 variables x1 to x20
            variable_name = f"x{i}"
            if self.id == 1 or i % 2 == 0 or (1 + i % 10) == self.id:
                # Initialize the variable if it belongs to the site based on your rules
                self.data[variable_name] = 10 * i
                self.commit_history[variable_name] = [(0, 10 * i)]  # Starting value is 10*i

    def is_up(self):
        # Returns whether the site is up (not failed)
        return self.status == "up"

    def is_failed(self):
        # Returns whether the site is in a failed state
        return self.status == "down"

    def is_variable_readable(self, variable, timestamp):
        # Determine if the variable is readable based on its replication status and site failure state
        var_index = int(variable.strip().lstrip('x'))
        if var_index % 2 == 1:
            # Non-replicated variable: check if the site is up
            return self.is_up()
        else:
            # Replicated variable: check if the site is up and has committed value
            return self.is_up() and (self.recovery_time is None or 
                                     self.get_last_committed_value(variable, timestamp) is not None)

    def has_failed_since(self, timestamp):
        # Returns True if the site failed after the given timestamp
        return any(fail_time > timestamp for fail_time in self.failure_history)

    def fail(self, fail_time):
        # Mark the site as down and record the failure time
        self.status = "down"
        self.failure_history.append(fail_time)
        self.recovery_time = None  # Set recovery time to None during failure

    def recover(self, recover_time):
        # Mark the site as up and set the recovery time
        self.status = "up"
        self.recovery_time = recover_time
        for variable in self.data:
            if int(variable.strip().lstrip('x')) % 2 == 0:
                # Mark replicated variables as unreadable during recovery
                self.mark_variable_unreadable(variable)
            else:
                # Non-replicated variables are available immediately after recovery
                self.commit_history[variable].append((recover_time, self.data[variable]))

    def mark_variable_unreadable(self, variable):
        # Mark a variable as unreadable by appending a "None" entry at the recovery time
        if variable in self.commit_history:
            self.commit_history[variable].append((self.recovery_time, None))
        else:
            self.commit_history[variable] = [(self.recovery_time, None)]

    def write(self, variable, value, timestamp):
        # Write a value for the variable at the current time
        if variable in self.commit_history:
            self.commit_history[variable].append((timestamp, value))
        else:
            self.commit_history[variable] = [(timestamp, value)]
        self.data[variable] = value

    def get_last_committed_value(self, variable, timestamp):
        # Retrieve the last committed value of a variable at or before the given timestamp
        if variable not in self.commit_history:
            # If the variable is not in the commit history, return the initial value
            return 10 * int(variable.strip().lstrip('x'))
        for commit_time, value in reversed(self.commit_history[variable]):
            if commit_time <= timestamp and value is not None:
                return value
        return 10 * int(variable.strip().lstrip('x'))  # Default value if no commit found
