## Latihan 2.1: Membaca dan Memilah File Mirip CSV Sederhana
# Buka file data.txt untuk membaca.
# Baca baris header-nya secara terpisah.
# Lakukan loop pada baris-baris selanjutnya. Untuk setiap baris, 
# pisahkan berdasarkan koma (,) lalu tampilkan Produk dan Kuantitas-nya.
# Simpan setiap catatan sebagai dictionary 
# (contoh: {'Product': 'Laptop', 'Quantity': 1, 'Price': 1200.00}) ke dalam sebuah list dictionary.


file_path = "data.txt"

try:
    with open(file_path, "r") as file:
        # membaca header file
        headers = file.readline()
        print("Headers file:", headers)

        # membuat header menjadi list
        headers = headers.strip().split(",")
        print("Headers after strip & split", headers)

        # membaca baris di bawah header
        lines = file.readlines()
        print("Isi file:", lines)

        data_dict = []
        for line in lines:
            line = line.strip()
            if not line: # skip empty lines
                continue
            print("Line:", line)
            values = line.split(",") # split by comma
            print("Values:", values) # values berisi baris yang menjadi list

            # membuat dictionary tiap baris
            line_dict = dict(zip(headers, values))

            # menyimpan tiap dictionary ke dalam list
            data_dict.append(line_dict)
        print(data_dict)

#-------------------------------------------------------------------------------------------
# # Latihan 2.2: Penyaringan Data Dasar dan Menulis ke File Baru
# Gunakan data yang sudah kalian pilah dari Latihan 2.1 (yaitu, list dictionary kalian).
# Saring produk yang Harganya lebih dari 100.00.
# Tulis produk-produk yang sudah disaring ini (contoh format: "Produk,Harga") 
# ke file baru bernama high_value_products.txt.

    high_value = []

    # menyaring produk dengan harga di atas 100.00
    for item in data_dict:
        if float(item["Price"]) > 100.00:
            high_value.append([item['Product'], item['Price']])
    print("Produk di atas 100.00", high_value)

    # menjadikan list berisi string untuk setiap itemnya
    data_to_write = [",".join(item) for item in high_value]
    print(data_to_write)

    # menambahkan header
    data_to_write.insert(0,"Produk,Harga")
    print(data_to_write)

    # menyimpan hasil ke dalam bentuk txt
    file_path = "high_value_products.txt"
    with open(file_path,"w") as file:
        for line in data_to_write:
            file.write(line + "\n")

except FileNotFoundError:
    print(f"❌ Error: file {file_path} tidak ditemukan")
except Exception as e:
    print(f"❌ Error: {e}")
