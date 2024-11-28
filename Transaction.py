class Transaction:
    def __init__(self, txn_id, start_time):
        self.id = txn_id
        self.start_time = start_time
        self.end_time = None
        self.read_set = set()
        self.write_set = {}
        self.status = "active"
        # print(f"DEBUG: Transaction {txn_id} initialized at time {start_time}")

    def add_read(self, variable):
        if not self.is_aborted():
            self.read_set.add(variable)
            # print(f"DEBUG: Added read of {variable} to transaction {self.id}")

    def add_write(self, variable, value):
        if not self.is_aborted():
            self.write_set[variable] = value
            # print(f"DEBUG: Added write of {variable}={value} to transaction {self.id}")

    def commit(self, end_time):
        self.status = "committed"
        self.end_time = end_time
        # print(f"DEBUG: Transaction {self.id} committed at time {end_time}")

    def abort(self):
        self.status = "aborted"
        self.read_set.clear()
        self.write_set.clear()
        # print(f"DEBUG: Transaction {self.id} aborted")

    def is_active(self):
        return self.status == "active"

    def is_waiting(self):
        return self.status == "waiting"

    def is_committed(self):
        return self.status == "committed"

    def is_aborted(self):
        return self.status == "aborted"

    def check_rw_conflict(self, other_txn):
        conflict = any(var in other_txn.write_set for var in self.read_set)
        if conflict:
            print(f"DEBUG: RW conflict detected between {self.id} and {other_txn.id}")
        return conflict