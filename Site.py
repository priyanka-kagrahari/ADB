class Site:
    def __init__(self, site_id: int):
        self.id = site_id
        self.status = "up"  # Can be 'up' or 'down'
        self.data = {}  # {variable_name: [versions]}
        self.commit_history = {}  # {variable_name: [(timestamp, value)]}
        self.recovery_time = None  # Tracks the last recovery time (if applicable)

    def initialize_data(self):
        # Populate site with initial data
        for i in range(1, 21):
            variable_name = f"x{i}"
            if i % 2 == 0 or (1 + i % 10) == self.id:  # Even or matches site
                self.data[variable_name] = 10 * i
                self.commit_history[variable_name] = [(0, 10 * i)]

    def is_up(self):
        return self.status == "up"

    def fail(self):
        self.status = "down"

    def recover(self):
        self.status = "up"
        self.recovery_time = None

    def get_last_committed_value(self, variable, timestamp):
        # Find the most recent commit for a variable before the given timestamp
        if variable not in self.commit_history:
            return None
        for commit_time, value in reversed(self.commit_history[variable]):
            if commit_time <= timestamp:
                return value
        return None

    def write(self, variable, value, timestamp):
        if variable in self.commit_history:
            self.commit_history[variable].append((timestamp, value))
        else:
            self.commit_history[variable] = [(timestamp, value)]
        self.data[variable] = value

    def read(self, variable):
        return self.data.get(variable, None)
