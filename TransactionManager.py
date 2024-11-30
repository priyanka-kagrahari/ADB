import time
from Site import Site
from Transaction import Transaction

class TransactionManager:
    def __init__(self):
        self.sites = {i: Site(i) for i in range(1, 11)}  # Sites from 1 to 10
        self.transactions = {}  # Tracks active transactions
        self.time = 0  # Simulated time for transaction timestamps
        self.committed_transactions = []  # List of all committed transactions

    def recover(self, site_id):
        """ Recover a site and reapply all committed transactions after recovery """
        site = self.sites[site_id]
        site.recover(self.time, self.committed_transactions)  # Apply committed transactions after recovery
        print(f"Site {site_id} recovers")

    def initialize_sites(self):
        """ Initialize all sites with their initial data """
        for site in self.sites.values():
            site.initialize_data()

    def begin(self, txn_id):
        """ Begin a new transaction """
        if txn_id in self.transactions:
            raise ValueError(f"Transaction {txn_id} already exists!")
        self.transactions[txn_id] = Transaction(txn_id, self.time)
        print(f"Transaction {txn_id} begins")
        self.advance_time()  # Advance time after transaction begins

    def read(self, txn_id, variable):
        """ Read a variable in a transaction """
        txn = self.transactions[txn_id]
        var_index = int(variable.strip().lstrip('x'))

        if var_index % 2 == 1:  # Non-replicated variable
            site_id = 1 + (var_index % 10)
            site = self.sites[site_id]
            if site.is_up():
                last_commit_time = site.get_last_commit_time(variable)
                if site.was_up_continuously_between(last_commit_time, txn.start_time):
                    value = site.get_last_committed_value(variable, txn.start_time)
                    if value is not None:
                        print(f"{variable}: {value}")
                        txn.add_read(variable)
                        txn.add_accessed_site(site_id)
                        return
                else:
                    print(f"Transaction {txn_id} cannot read {variable}; site {site_id} was down during required interval")
                    txn.abort()
                    return
            else:
                print(f"Transaction {txn_id} cannot read {variable}; site {site_id} is down")
                txn.abort()
                return

        else:  # Replicated variable
            read_successful = False
            for site in self.sites.values():
                if site.is_up() and variable in site.data:
                    last_commit_time = site.get_last_commit_time(variable)
                    if site.was_up_continuously_between(last_commit_time, txn.start_time):
                        if site.is_variable_readable(variable, txn.start_time):
                            value = site.get_last_committed_value(variable, txn.start_time)
                            if value is not None:
                                print(f"{variable}: {value}")
                                txn.add_read(variable)
                                txn.add_accessed_site(site.id)
                                read_successful = True
                                break
            if not read_successful:
                print(f"Transaction {txn_id} cannot read {variable}; no site has it available")
                txn.abort()
                return
        self.advance_time()

    def write(self, txn_id, variable, value):
        """ Write a value to a variable in a transaction """
        txn = self.transactions[txn_id]
        txn.add_write(variable, value)
        affected_sites = [site.id for site in self.sites.values() if site.is_up() and variable in site.data]
        txn.add_accessed_sites(affected_sites)
        print(f"{txn_id} writes {variable}: {value} at sites {affected_sites}")
        self.advance_time()

    def end(self, txn_id):
        """ End a transaction and commit or abort it """
        txn = self.transactions[txn_id]
        self.debug_log(f"Transaction {txn.id} | Status: {txn.status} | Action: Called end()")
        txn.set_end_time(self.time)  # Set the end time of the transaction

        if txn.is_aborted():
            # Transaction has already been aborted
            print(f"Transaction {txn_id} aborts")
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Transaction already aborted")
            return

        # Validate and commit transaction
        if self.validate_transaction(txn):
            txn.commit(self.time)
            self.committed_transactions.append(txn)

            # Debugging: Log commit time and writes
            self.debug_log(f"Transaction {txn.id} commits at time {self.time}. Write set: {txn.write_set}")

            # Write the committed values to the sites
            for variable, value in txn.write_set.items():
                for site in self.sites.values():
                    if site.is_up() and variable in site.data:
                        self.debug_log(f"Transaction {txn.id} writing {variable}: {value} to site {site.id}")
                        site.write(variable, value, self.time)

            print(f"Transaction {txn_id} commits")
            self.debug_log(f"Transaction {txn.id} | Status: committed | Action: Transaction committed")
        else:
            txn.abort()
            print(f"Transaction {txn_id} aborts")
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Transaction aborted during validation")

    def validate_transaction(self, txn):
        """
        Validates a transaction to ensure it maintains serializable snapshot isolation (SSI).
        Detects serialization anomalies involving the transaction.
        """
        # Build edges in the serialization graph involving txn
        serialization_edges = []
        for other_txn in self.committed_transactions:
            # Skip transactions that ended before txn started
            if other_txn.end_time <= txn.start_time:
                continue
            # Edge from other_txn to txn if other_txn writes data read by txn (RW conflict)
            if other_txn.check_write_read_conflict(txn):
                serialization_edges.append((other_txn.id, txn.id))
            # Edge from txn to other_txn if txn writes data read by other_txn (WR conflict)
            if txn.check_write_read_conflict(other_txn):
                serialization_edges.append((txn.id, other_txn.id))
            # Edge from other_txn to txn if other_txn writes before txn writes the same variable (WW conflict)
            if other_txn.check_write_write_conflict(txn):
                    if txn.start_time < other_txn.start_time:
                        # Serialization anomaly: txn must be serialized after other_txn but started before
                        self.debug_log(f"Transaction {txn.id} | Status: active | Action: Serialization anomaly detected with transaction {other_txn.id} due to WW conflict")
                        return False
                    else:
                        # Add edge from txn to other_txn
                        serialization_edges.append((txn.id, other_txn.id))

        # Check for cycles involving txn
        if self.has_cycle(serialization_edges, txn.id):
            self.debug_log(f"Transaction {txn.id} | Status: active | Action: Cycle detected in serialization graph")
            return False

        # Additional check for serialization anomalies due to snapshot order
        for other_txn in self.committed_transactions:
            if other_txn.end_time <= txn.start_time:
                continue
            if other_txn.start_time > txn.start_time:
                # If txn must be serialized before other_txn due to conflicts, but started earlier, it's okay
                continue
            # If txn must be serialized after other_txn but started before, it may be a serialization anomaly
            if (other_txn.id, txn.id) in serialization_edges:
                self.debug_log(f"Transaction {txn.id} | Status: active | Action: Serialization anomaly detected with transaction {other_txn.id}")
                return False

        return True

    def has_cycle(self, edges, start_txn_id):
        from collections import defaultdict, deque

        graph = defaultdict(set)
        for src, dst in edges:
            graph[src].add(dst)

        visited = set()
        stack = set()

        def visit(txn_id):
            if txn_id in stack:
                return True  # Cycle detected
            if txn_id in visited:
                return False
            visited.add(txn_id)
            stack.add(txn_id)
            for neighbor in graph[txn_id]:
                if visit(neighbor):
                    return True
            stack.remove(txn_id)
            return False

        return visit(start_txn_id)

    def fail(self, site_id):
        """ Fail a site and handle any transactions that were affected by the failure """
        site = self.sites[site_id]
        site.fail(self.time)
        print(f"Site {site_id} fails")

        # Abort transactions that accessed the failed site
        for txn in self.transactions.values():
            if txn.is_active() and site_id in txn.accessed_sites:
                txn.abort()
                print(f"Transaction {txn.id} aborts due to site {site_id} failure")

    def dump(self):
        """ Print the current state of all sites and their data """
        for site_id, site in self.sites.items():
            variables = []
            for var in sorted(site.data.keys(), key=lambda x: int(x[1:])):  # Sort variables by index
                if site_id == 1 or int(var[1:]) % 2 == 0 or (1 + int(var[1:]) % 10) == site_id:
                    value = site.get_last_committed_value(var, self.time)
                    variables.append(f"{var}: {value}")
            print(f"site {site_id} - " + ", ".join(variables))

    def advance_time(self):
        """ Advance the simulated time by one unit """
        self.time += 1

    def debug_log(self, message):
        """ Outputs debug messages for tracing execution """
        print(f"DEBUG: {message}")
