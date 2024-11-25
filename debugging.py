def querystate(self):
    """
    Print the state of all transactions and sites for debugging purposes.
    """
    print("Active Transactions:")
    for txn_id, txn in self.transactions.items():
        print(f"{txn_id}: {txn.status}, Reads: {txn.read_set}, Writes: {txn.write_set}")
    
    print("\nSite Status:")
    for site_id, site in self.sites.items():
        print(f"Site {site_id}: {site.status}, Data: {site.data}")
