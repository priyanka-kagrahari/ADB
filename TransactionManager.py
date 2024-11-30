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

    def read(self, txn_id, variable):
        """ Read a variable in a transaction """
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


    def write(self, txn_id, variable, value):
        """ Write a value to a variable in a transaction """
        txn = self.transactions[txn_id]
        txn.add_write(variable, value)
        affected_sites = [site.id for site in self.sites.values() if site.is_up() and variable in site.data]
        txn.add_accessed_sites(affected_sites)
        print(f"{txn_id} writes {variable}: {value} at sites {affected_sites}")

    def end(self, txn_id):
        """ End a transaction and commit or abort it """
        txn = self.transactions[txn_id]
        self.debug_log(f"Transaction {txn.id} | Status: {txn.status} | Action: Called end()")

        if txn.is_aborted():
            # Transaction has already been aborted
            print(f"Transaction {txn_id} aborts")
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Transaction already aborted")
            return

        # **Enhanced Waiting and Recovery Logic during Commit**
        # Check if transaction should abort due to failed sites
        should_abort = False
        for site_id in txn.accessed_sites:
            site = self.sites[site_id]
            self.debug_log(f"Transaction {txn.id} accessing site {site_id} (status: {'failed' if site.is_failed() else 'up'})")

            if site.is_failed():
                # If the site failed before or during the transaction and has not recovered, abort
                if txn.start_time <= site.recovery_time:
                    should_abort = True
                    self.debug_log(f"Transaction {txn.id} | Site {site_id} failed before or during the transaction; aborting.")
                    break
                # If the site is in recovery, we can't commit until recovery is complete
                elif site.recovery_time is not None and site.recovery_time > txn.end_time:
                    should_abort = True
                    self.debug_log(f"Transaction {txn.id} | Site {site_id} in recovery (recovery_time: {site.recovery_time}); aborting.")
                    break

        if should_abort:
            txn.abort()
            self.debug_log(f"Transaction {txn.id} | Status: aborted | Action: Aborted due to failed/recovering site")
            print(f"Transaction {txn_id} aborts")
            return

        # Wait for site recovery if needed
        for site_id in txn.accessed_sites:
            site = self.sites[site_id]
            if site.is_failed() and site.recovery_time is None:
                self.debug_log(f"Transaction {txn.id} | Waiting for site {site_id} to recover.")
                while site.is_failed():
                    time.sleep(1)  # Sleep for a short time before checking again

        # Once recovery is complete for all affected sites, proceed with commit
        for site_id in txn.accessed_sites:
            site = self.sites[site_id]
            if site.is_failed() or site.recovery_time is not None:
                self.debug_log(f"Transaction {txn.id} | Site {site_id} still failed during commit; aborting.")
                txn.abort()
                print(f"Transaction {txn_id} aborts")
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
                    if site.is_up() and variable in site.data and site.recovery_time is None:
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
        Checks for RW and WW conflicts with previously committed transactions.
        """
        for other_txn in self.committed_transactions:
            if other_txn.end_time > txn.start_time:
                if txn.check_rw_conflict(other_txn):
                    self.debug_log(f"Transaction {txn.id} | Status: active | Action: RW conflict with {other_txn.id}")
                    return False
                if other_txn.check_rw_conflict(txn):
                    self.debug_log(f"Transaction {txn.id} | Status: active | Action: WW conflict with {other_txn.id}")
                    return False

        return True

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
