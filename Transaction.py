class Transaction:
    def __init__(self, txn_id: str, start_time: int):
        self.id = txn_id
        self.start_time = start_time
        self.read_set = set()
        self.write_set = {}  # {variable: value}
        self.status = "active"

    def add_read(self, variable):
        self.read_set.add(variable)

    def add_write(self, variable, value):
        self.write_set[variable] = value

    def abort(self):
        self.status = "aborted"

    def commit(self):
        self.status = "committed"
