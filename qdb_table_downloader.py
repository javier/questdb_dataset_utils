import requests
import csv


def download_query(query):
    with open("output.csv", "w") as outfile:
        writer = csv.writer(outfile)
        last_line = None
        page = 0
        while last_line != 0:
            row_from = page * 50000
            row_to = (page + 1) * 50000
            resp = requests.get('https://demo.questdb.io/exp', {'query': query, 'limit': f"{row_from},{row_to}"})
            decoded_content = resp.content.decode('utf-8')
            csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
            headers = next(csv_reader, None)
            if page == 0:
                writer.writerow(headers)
            last_line = 0
            for row in csv_reader:
                writer.writerow(row)
                last_line = csv_reader.line_num
            print(last_line)
            page += 1


if __name__ == '__main__':
    download_query("SELECT * from weather")

