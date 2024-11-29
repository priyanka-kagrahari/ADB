from Transaction import Transaction
from Site import Site

class TransactionManager:
    def __init__(self):
        self.sites = {i: Site(i) for i in range(1, 11)}
        self.transactions = {}
        self.time = 0
        self.committed_transactions = []

    def initialize_sites(self):
        for site in self.sites.values():
            site.initialize_data()

    def begin(self, txn_id):
        if txn_id in self.transactions:
            raise ValueError(f"Transaction {txn_id} already exists!")
        self.transactions[txn_id] = Transaction(txn_id, self.time)
        print(f"{txn_id} begins")

    def read(self, txn_id, variable):
        txn = self.transactions[txn_id]
        var_index = int(variable.strip().lstrip('x'))

        if var_index % 2 == 1:  # Non-replicated variable
            site_id = 1 + (var_index % 10)
            site = self.sites[site_id]
            if site.is_up():
                value = site.get_last_committed_value(variable, txn.start_time)
                if value is not None:
                    print(f"{variable}: {value}")
                    txn.add_read(variable)
                    txn.add_accessed_site(site_id)
                    return
        else:  # Replicated variable
            for site in self.sites.values():
                if site.is_up() and site.is_variable_readable(variable, txn.start_time):
                    value = site.get_last_committed_value(variable, txn.start_time)
                    if value is not None:
                        print(f"{variable}: {value}")
                        txn.add_read(variable)
                        txn.add_accessed_site(site.id)
                        return

        print(f"Transaction {txn_id} waits for {variable}")
        txn.status = "waiting"

    def write(self, txn_id, variable, value):
        txn = self.transactions[txn_id]
        txn.add_write(variable, value)
        affected_sites = [site.id for site in self.sites.values() if site.is_up() and variable in site.data]
        txn.add_accessed_sites(affected_sites)
        print(f"{txn_id} writes {variable}: {value} at sites {affected_sites}")

    def end(self, txn_id):
        txn = self.transactions[txn_id]
        self.debug_log(f"Transaction {txn.id} | Status: {txn.status} | Action: Called end()")
        
        if txn.is_aborted():
            print(f"{txn_id} aborts")
            return
        
        # Check if transaction should abort due to failed sites
        should_abort = False
        for site_id in txn.accessed_sites:
            site = self.sites[site_id]
            if site.is_failed() and txn.start_time <= site.recovery_time:
                should_abort = True
                break

        if should_abort:
            txn.abort()
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Aborted due to dependency on failed sites")
            print(f"{txn_id} aborts")
            return

        # Validate and commit transaction
        if self.validate_transaction(txn):
            txn.commit(self.time)
            self.committed_transactions.append(txn)
            for variable, value in txn.write_set.items():
                for site in self.sites.values():
                    if site.is_up() and variable in site.data and site.recovery_time is None:
                        site.write(variable, value, self.time)
            print(f"{txn_id} commits")
            self.debug_log(f"Transaction {txn.id} | Status: committed | Action: Transaction committed")
        else:
            txn.abort()
            print(f"{txn_id} aborts")
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Transaction aborted during validation")

    def validate_transaction(self, txn):
        """
        Validates a transaction to ensure it maintains serializable snapshot isolation (SSI).
        Checks for RW and WW conflicts with previously committed transactions.
        """
        for other_txn in self.committed_transactions:
            # Only check transactions that overlap in time
            if other_txn.end_time > txn.start_time:
                # Rule 1: RW Conflict
                if txn.check_rw_conflict(other_txn):
                    self.debug_log(f"Transaction {txn.id} | Status: active | Action: RW conflict with {other_txn.id}")
                    return False

                # Rule 2: WW Conflict
                if other_txn.check_rw_conflict(txn):
                    self.debug_log(f"Transaction {txn.id} | Status: active | Action: WW conflict with {other_txn.id}")
                    return False

        # No conflicts found
        return True

    def fail(self, site_id):
        site = self.sites[site_id]
        site.fail(self.time)
        print(f"Site {site_id} fails")

        # Abort transactions that accessed the failed site
        for txn in self.transactions.values():
            if txn.is_active() and site_id in txn.accessed_sites:
                txn.abort()
                print(f"{txn.id} aborts due to site {site_id} failure")

    def recover(self, site_id):
        site = self.sites[site_id]
        site.recover(self.time)
        print(f"Site {site_id} recovers")

    def dump(self):
        for site_id, site in self.sites.items():
            variables = []
            for var in sorted(site.data.keys(), key=lambda x: int(x[1:])):
                if site_id == 1 or int(var[1:]) % 2 == 0 or (1 + int(var[1:]) % 10) == site_id:
                    value = site.get_last_committed_value(var, self.time)
                    variables.append(f"{var}: {value}")
            print(f"site {site_id} - " + ", ".join(variables))

    def advance_time(self):
        self.time += 1

    def debug_log(self, message):
        """
        Outputs debug messages for tracing execution.
        """
        print(f"DEBUG: {message}")
