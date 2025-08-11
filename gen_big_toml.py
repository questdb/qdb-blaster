datatypes = ["Double", "Long", "Symbol"]

def generate_schema():
    schema_lines = ['    ["timestamp", "Timestamp"],']
    for i in range(1, 41):
        dtype = datatypes[(i - 1) % len(datatypes)]
        schema_lines.append(f'    ["col{i}", "{dtype}"],')
    # Remove trailing comma from last line
    schema_lines[-1] = schema_lines[-1].rstrip(',')
    return "[\n" + "\n".join(schema_lines) + "\n]"

def generate_send_section():
    return '''batch_pause = ["10ms", "100ms"]
batch_size = [10000, 50000]
parallel_senders = 4
tot_rows = 25_000_000
batches_connection_keepalive = 10
'''

def main():
    with open("big.toml", "w") as f:
        # Write header
        f.write('debug = true\n\n')
        f.write('[database]\n')
        f.write('ilp = "http::addr=localhost:9000;token=qt1cBkOuvc_8VFCMHRacRaDNp7DkzTtf9Cu1eh6rSuYfMM;"\n')
        f.write('pgsql = "host=localhost port=8812 user=test_user password=pass dbname=qdb"\n\n')

        schema = generate_schema()
        send_section = generate_send_section()

        for t in range(1, 21):
            f.write(f'[tables.metrics{t}]\n')
            f.write(f'schema = {schema}\n')
            f.write('designated_ts = "timestamp"\n\n')
            f.write(f'[tables.metrics{t}.send]\n')
            f.write(send_section)
            f.write('\n')

if __name__ == "__main__":
    main()
