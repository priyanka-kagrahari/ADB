class Transaction:
    def __init__(self, txn_id, start_time):
        self.id = txn_id
        self.start_time = start_time
        self.end_time = None
        self.read_set = set()
        self.write_set = {}
        self.accessed_sites = set()  # Track sites accessed by this transaction
        self.status = "active"

    def add_read(self, variable):
        if not self.is_aborted():
            self.read_set.add(variable)

    def add_write(self, variable, value):
        if not self.is_aborted():
            self.write_set[variable] = value

    def add_accessed_site(self, site_id):
        self.accessed_sites.add(site_id)

    def add_accessed_sites(self, site_ids):
        self.accessed_sites.update(site_ids)

    def commit(self, end_time):
        self.status = "committed"
        self.end_time = end_time

    def abort(self):
        self.status = "aborted"
        self.read_set.clear()
        self.write_set.clear()
        self.accessed_sites.clear()

    def is_active(self):
        return self.status == "active"

    def is_aborted(self):
        return self.status == "aborted"

    def check_rw_conflict(self, other_txn):
        return any(var in other_txn.write_set for var in self.read_set)
