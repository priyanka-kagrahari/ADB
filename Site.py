class Site:
    def __init__(self, site_id):
        self.id = site_id
        self.status = "up"
        self.data = {}
        self.commit_history = {}
        self.recovery_time = None
        self.failure_history = []
        # print(f"DEBUG: Site {site_id} initialized")

    def initialize_data(self):
        for i in range(1, 21):
            variable_name = f"x{i}"
            if i % 2 == 0 or (1 + i % 10) == self.id:
                self.data[variable_name] = 10 * i
                self.commit_history[variable_name] = [(0, 10 * i)]
        # print(f"DEBUG: Data initialized for site {self.id}")

    def is_up(self):
        return self.status == "up"

    def is_variable_readable(self, variable, timestamp):
        var_index = int(variable.strip().lstrip('x'))
        # print(f"DEBUG: Checking readability of {variable} at site {self.id}")
        if var_index % 2 == 1:
            return self.is_up()
        else:
            return self.is_up() and (self.recovery_time is None or 
                   self.get_last_committed_value(variable, timestamp) is not None)

    def has_failed_since(self, timestamp):
        return any(fail_time > timestamp for fail_time in self.failure_history)

    def fail(self, fail_time):
        self.status = "down"
        self.failure_history.append(fail_time)
        # print(f"DEBUG: Site {self.id} failed at time {fail_time}")

    def recover(self, recover_time):
        self.status = "up"
        self.recovery_time = recover_time
        # print(f"DEBUG: Site {self.id} recovered at time {recover_time}")
        for var in self.data:
            if int(var.strip().lstrip('x')) % 2 == 0:
                self.data[var] = None   

    def write(self, variable, value, timestamp):
        if variable in self.commit_history:
            self.commit_history[variable].append((timestamp, value))
        else:
            self.commit_history[variable] = [(timestamp, value)]
        self.data[variable] = value
        # print(f"DEBUG: Write {variable}={value} at site {self.id} at time {timestamp}")

    def get_last_committed_value(self, variable, timestamp):
        var_index = int(variable.strip().lstrip('x'))
        if variable not in self.commit_history:
            if var_index % 2 == 1:
                return 10 * var_index  # Initial value for odd-indexed variables
            return None
        for commit_time, value in reversed(self.commit_history[variable]):
            if commit_time <= timestamp:
                return value
        if var_index % 2 == 1:
            return 10 * var_index  # Initial value for odd-indexed variables
        return None