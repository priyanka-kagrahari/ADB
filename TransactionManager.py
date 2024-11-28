from Transaction import Transaction
from Site import Site

class TransactionManager:
    def __init__(self):
        self.sites = {i: Site(i) for i in range(1, 11)}
        self.transactions = {}
        self.time = 0
        self.committed_transactions = []
        # print("DEBUG: TransactionManager initialized")

    def initialize_sites(self):
        for site in self.sites.values():
            site.initialize_data()
        # print("DEBUG: All sites initialized")

    def begin(self, txn_id):
        if txn_id in self.transactions:
            raise ValueError(f"Transaction {txn_id} already exists!")
        self.transactions[txn_id] = Transaction(txn_id, self.time)
        print(f"{txn_id} begins")
        # print(f"DEBUG: Transaction {txn_id} started at time {self.time}")

    def read(self, txn_id, variable):
        txn = self.transactions[txn_id]
        var_index = int(variable.strip().lstrip('x'))
        # print(f"DEBUG: Attempting to read {variable} for transaction {txn_id}")
        
        if var_index % 2 == 1:
            site_id = 1 + (var_index % 10)
            site = self.sites[site_id]
            # print(f"DEBUG: {variable} is at site {site_id}")
            if site.is_up():
                value = site.get_last_committed_value(variable, txn.start_time)
                if value is not None:
                    print(f"{variable}: {value}")
                    txn.add_read(variable)
                    return
        else:
            for site in self.sites.values():
                if site.is_up() and site.is_variable_readable(variable, txn.start_time):
                    value = site.get_last_committed_value(variable, txn.start_time)
                    if value is not None:
                        print(f"{variable}: {value}")
                        txn.add_read(variable)
                        return
        
        print(f"Transaction {txn_id} waits for {variable}")
        txn.status = "waiting"
        
    def end(self, txn_id):
        txn = self.transactions[txn_id]
        if txn.is_aborted():
            print(f"{txn_id} aborts")
            return

        should_abort = False
        for variable in txn.write_set:
            var_index = int(variable.strip().lstrip('x'))
            if var_index % 2 == 1:
                site_id = 1 + (var_index % 10)
                if self.sites[site_id].has_failed_since(txn.start_time):
                    should_abort = True
                    break
            else:
                for site in self.sites.values():
                    if variable in site.data and site.has_failed_since(txn.start_time):
                        should_abort = True
                        break
            if should_abort:
                break

        if should_abort or not self.validate_transaction(txn):
            txn.abort()
            print(f"{txn_id} aborts")
        else:
            txn.commit(self.time)
            self.committed_transactions.append(txn)
            for variable, value in txn.write_set.items():
                for site in self.sites.values():
                    if site.is_up() and variable in site.data:
                        site.write(variable, value, self.time)
            print(f"{txn_id} commits")

    def write(self, txn_id, variable, value):
        txn = self.transactions[txn_id]
        txn.add_write(variable, value)
        # print(f"DEBUG: Transaction {txn_id} writing {variable}={value}")

        affected_sites = []
        for site in self.sites.values():
            if site.is_up() and variable in site.data:
                affected_sites.append(site.id)
        print(f"{txn_id} writes {variable}: {value} at sites {affected_sites}")

    def validate_transaction(self, txn):
        for other_txn in self.committed_transactions:
            if other_txn.end_time > txn.start_time:
                if txn.check_rw_conflict(other_txn) or other_txn.check_rw_conflict(txn):
                    # print(f"DEBUG: Validation failed for {txn.id} due to conflict with {other_txn.id}")
                    return False
        return True

    def fail(self, site_id):
        site = self.sites[site_id]
        site.fail(self.time)
        print(f"Site {site_id} fails")
        # print(f"DEBUG: Site {site_id} failed at time {self.time}")

    def recover(self, site_id):
        site = self.sites[site_id]
        site.recover(self.time)
        print(f"Site {site_id} recovers")
        # print(f"DEBUG: Site {site_id} recovered at time {self.time}")

    def dump(self):
        for site_id, site in self.sites.items():
            variables = sorted(site.data.items(), key=lambda x: int(x[0][1:]))
            print(f"site {site_id} - " + ", ".join([f"{var}: {val}" for var, val in variables]))
        # print("DEBUG: Dump completed")

    def advance_time(self):
        self.time += 1
        # print(f"DEBUG: Time advanced to {self.time}")