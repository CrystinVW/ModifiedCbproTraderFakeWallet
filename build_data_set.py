import csv


def build_csv(file_path, data):

    with open(file_path, 'a') as file:
        writer = csv.writer(file)
        writer.writerow(data)
        file.close()

    len_check = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            len_check.append(row)
        file.close()
    if len(len_check) > 300000:
        with open(file_path, 'w') as file:
            writer = csv.writer(file)
            num = 0
            for element in len_check:
                if num > 500:
                    writer.writerow(element)
                num += 1
            file.close()


def build_csv_dict(file_path, data):
    csv_columns = ['type', 'order_id', 'side', 'order_type', 'size',
                   'client_oid', 'time', 'price']
    with open(file_path, 'a') as file:
        line = []
        writer = csv.writer(file)

        for key in data.keys():
            line.append((key, data[key]))
        #writer.writerow(line)
            file.write("%s,%s\n" % (key, data[key]))

        #writer = csv.DictWriter(file, fieldnames=csv_columns)
        #for dictionary in data:
            #writer.writerow(dictionary)
        #writer.writerow(data)
        file.close()


    len_check = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            len_check.append(row)
        file.close()
    if len(len_check) > 50000:
        with open(file_path, 'w') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"')
            num = 0
            for element in len_check:
                if num > 500:
                    writer.writerow(element)
                num += 1
            file.close()