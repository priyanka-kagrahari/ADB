import sys
from TransactionManager import TransactionManager

def main(input_file):
    tm = TransactionManager()
    tm.initialize_sites()

    # Open the input file
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:  # Skip empty lines
                continue

            # Parse and execute commands
            parts = line.split("(")
            cmd = parts[0]
            args = parts[1].rstrip(")").split(",")

            if cmd == "begin":
                tm.begin(args[0])
            elif cmd == "R":
                tm.read(args[0], args[1])
            elif cmd == "W":
                tm.write(args[0], args[1], int(args[2]))
            elif cmd == "end":
                tm.end(args[0])
            elif cmd == "fail":
                tm.fail(int(args[0]))
            elif cmd == "recover":
                tm.recover(int(args[0]))
            elif cmd == "dump":
                tm.dump()

            # Increment the logical time after each command
            tm.time += 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python driver.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    main(input_file)
