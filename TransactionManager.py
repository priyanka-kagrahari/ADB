from Transaction import Transaction
from Site import Site

class TransactionManager:
    def __init__(self):
        self.sites = {i: Site(i) for i in range(1, 11)}
        self.transactions = {}  # {txn_id: Transaction}
        self.time = 0  # Logical clock
        self.committed_transactions = []  # Tracks committed transactions for validation

    def initialize_sites(self):
        for site in self.sites.values():
            site.initialize_data()

    def begin(self, txn_id: str):
        if txn_id in self.transactions:
            raise ValueError(f"Transaction {txn_id} already exists!")
        self.transactions[txn_id] = Transaction(txn_id, self.time)
        print(f"{txn_id} begins")

    def read(self, txn_id: str, variable: str):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} does not exist!")

        txn = self.transactions[txn_id]
        txn.add_read(variable)

        # Locate the variable across available sites
        for site in self.sites.values():
            if site.is_up() and variable in site.data:
                # Prevent reads from recovering sites for replicated variables
                if site.recovery_time and variable in site.commit_history:
                    print(f"Transaction {txn.id} waits for {variable} to be readable at site {site.id}")
                    txn.status = "waiting"
                    return

                # Get the last committed value before txn's start time
                value = site.get_last_committed_value(variable, txn.start_time)
                if value is not None:
                    print(f"{variable}: {value}")
                    return

        print(f"Transaction {txn_id} waits for {variable}")
        txn.status = "waiting"

    def write(self, txn_id: str, variable: str, value: int):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} does not exist!")

        txn = self.transactions[txn_id]
        txn.add_write(variable, value)

        # Write to all available copies
        affected_sites = []
        for site in self.sites.values():
            if site.is_up() and variable in site.data:
                site.write(variable, value, self.time)
                affected_sites.append(site.id)
        print(f"{txn_id} writes {variable}={value} to sites {affected_sites}")

    def validate_transaction(self, txn: Transaction):
        """
        Validate a transaction using SSI rules.
        - Abort if written variables are affected by failed sites.
        - First Committer Wins: Abort if WW conflict occurs.
        - Serialization Graph Cycles: Detect consecutive RW conflicts.
        """
        # Check for failed sites affecting writes
        for site_id, site in self.sites.items():
            if not site.is_up() and any(var in txn.write_set for var in site.data.keys()):
                print(f"Transaction {txn.id} aborts due to site {site_id} failure affecting writes.")
                return False

        # SSI validation
        for committed_txn in self.committed_transactions:
            # Skip committed transactions that don't overlap with txn
            if committed_txn.start_time > txn.start_time:
                continue

            # Rule 1: WW Conflict (First Committer Wins)
            if any(var in txn.write_set for var in committed_txn.write_set):
                print(f"Transaction {txn.id} aborts due to WW conflict with {committed_txn.id}")
                return False

            # Rule 2: RW Edge Conflict
            if any(var in txn.read_set for var in committed_txn.write_set):
                print(f"Transaction {txn.id} aborts due to RW conflict with {committed_txn.id}")
                return False

            # Rule 3: Cycles in the Serialization Graph
            if self.detect_cycle(txn, committed_txn):
                print(f"Transaction {txn.id} aborts due to a serialization cycle")
                return False

        return True

    def detect_cycle(self, txn: Transaction, committed_txn: Transaction):
        """
        Detect cycles in the serialization graph. This is an approximation
        since we don't build a full graph but detect consecutive RW conflicts.
        """
        for var in txn.read_set:
            if var in committed_txn.write_set:
                return True  # RW cycle detected
        return False

    def end(self, txn_id: str):
        if txn_id not in self.transactions:
            raise ValueError(f"Transaction {txn_id} does not exist!")

        txn = self.transactions[txn_id]

        # Validate and decide whether to commit or abort
        if self.validate_transaction(txn):
            txn.commit()
            print(f"{txn_id} commits")
            self.committed_transactions.append(txn)  # Track committed transactions
            # Apply writes to the sites
            for variable, value in txn.write_set.items():
                for site in self.sites.values():
                    if site.is_up() and variable in site.data:
                        site.write(variable, value, self.time)
        else:
            txn.abort()
            print(f"{txn_id} aborts")

    def fail(self, site_id: int):
        """
        Mark a site as down.
        """
        if site_id not in self.sites:
            raise ValueError(f"Site {site_id} does not exist!")
        site = self.sites[site_id]
        site.fail()
        print(f"Site {site_id} fails")

    def recover(self, site_id: int):
        """
        Mark a site as up and initialize recovery logic for replicated variables.
        """
        if site_id not in self.sites:
            raise ValueError(f"Site {site_id} does not exist!")
        site = self.sites[site_id]
        site.recover()
        print(f"Site {site_id} recovers")

    def dump(self):
        """
        Print committed values of all variables at all sites.
        """
        for site_id, site in self.sites.items():
            print(f"site {site_id} - {site.data}")
