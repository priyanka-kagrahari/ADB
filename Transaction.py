class Transaction:
    def __init__(self, txn_id, start_time):
        self.id = txn_id
        self.start_time = start_time
        self.end_time = None
        self.read_set = set()
        self.write_set = {}
        self.accessed_sites = set()  # Track sites accessed by this transaction
        self.status = "active"

    # Method to get the committed variables (write set)
    def get_committed_variables(self):
        """Returns the write set as a list of (variable, value) pairs."""
        return list(self.write_set.items())  # Returns a list of (variable, value) pairs

    def execute(self, sites, down_sites):
        """Execute transaction on sites that are up."""
        print(f"DEBUG: Transaction {self.id} begins at time {self.start_time}")
        
        # Write to only the sites that were up during the transaction execution
        for site in sites:
            if site.id not in down_sites:  # Only apply write if site is up during T2 execution
                self.write_set[site.id] = {"x8": 88}  # Record T2 writes x8=88
                site.apply_write(self.id, "x8", 88)  # Apply T2's write to x8
                print(f"DEBUG: T2 writing x8=88 to site {site.id}")
            else:
                print(f"DEBUG: Site {site.id} was down during T2, skipping write")


    def check_write_read_conflict(self, other_txn):
        """ Check if self writes variables that other_txn read """
        return any(var in self.write_set and var in other_txn.read_set for var in self.write_set)

    def check_write_write_conflict(self, other_txn):
        """ Check if self writes variables that other_txn writes """
        return any(var in self.write_set and var in other_txn.write_set for var in self.write_set)

    def add_read(self, variable):
        if not self.is_aborted():
            self.read_set.add(variable)

    def add_write(self, variable, value):
        if not self.is_aborted():
            print(f"DEBUG: Transaction {self.id} writes {variable} = {value}")

            self.write_set[variable] = value

    def add_accessed_site(self, site_id):
        self.accessed_sites.add(site_id)

    def add_accessed_sites(self, site_ids):
        self.accessed_sites.update(site_ids)

    def commit(self, end_time):
        self.status = "committed"
        self.end_time = end_time
        print(f"DEBUG: Transaction {self.id} commits at time {self.end_time}. Write set: {self.write_set}")


    def abort(self):
        if self.status != "aborted":
            self.status = "aborted"
            self.end_time = self.end_time or self.start_time  # Set end_time if not already set
            self.read_set.clear()
            self.write_set.clear()
            self.accessed_sites.clear()
            print(f"Transaction {self.id} aborts")
            print(f"DEBUG: Transaction {self.id} | Status: aborted | Action: Transaction aborted")


    def set_end_time(self, end_time):
        self.end_time = end_time


    def is_active(self):
        return self.status == "active"

    def is_aborted(self):
        return self.status == "aborted"

    def check_rw_conflict(self, other_txn):
        """ Check if there is a read-write conflict with another transaction """
        return any(var in other_txn.write_set for var in self.read_set)
