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
    return '''batch_pause = ["50ms", "100ms"]
batch_size = [5000, 10000]
parallel_senders = 16
tot_rows = 15_000_000
batches_connection_keepalive = 20
'''

def main():
    with open("big.toml", "w") as f:
        # Write header
        f.write('debug = true\n\n')
        f.write('[database]\n')
        #f.write('ilp = "http::addr=localhost:9000;token=qt1cBkOuvc_8VFCMHRacRaDNp7DkzTtf9Cu1eh6rSuYfMM;"\n')
        f.write(
            'ilp = "tcp::addr=localhost:9009;protocol_version=2;'
            'username=test_ilp_user;'
            'token=NPo-BO1F2zr7LoWBEeBpY6ZfNQXRdhpyud3llmWUycg;'
            'token_x=ayc3ehklyXSa4Pe1w-jVxqGZjh0jJMUECThWftnHnu0;'
            'token_y=N0fRjqck8MrwcvLCuJK_lQexLaDV8ZSn-EKnNHsBX1o"\n')
        f.write('pgsql = "host=localhost port=8812 user=test_user password=pass dbname=qdb"\n\n')

        schema = generate_schema()
        send_section = generate_send_section()

        for t in range(1, 11):
            f.write(f'[tables.metrics{t}]\n')
            f.write(f'schema = {schema}\n')
            f.write('designated_ts = "timestamp"\n\n')
            f.write(f'[tables.metrics{t}.send]\n')
            f.write(send_section)
            f.write('\n')

if __name__ == "__main__":
    main()
