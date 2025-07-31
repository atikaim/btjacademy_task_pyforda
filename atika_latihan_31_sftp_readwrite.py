# --------------------------------------------------------------------------------------------------
## Latihan 3.1: Proses File yang Diunduh Pakai Python Dasar
# Unduh file sales_data.csv.
# Setelah berhasil mengunduh file sales_data.csv.
# Buka file lokal ini menggunakan fungsi penanganan file dasar Python
# Baca isinya baris per baris, pisahkan berdasarkan koma, 
# dan tampilkan nama Produk serta Kuantitas untuk setiap catatan.
# Buat file csv baru dengan tambahan kolom dari sales_data itu 
# ditambahkan total_amount -> diambil dari quantity * price
# Upload ke server SFTP dengan nama <nama>_sales_data.csv (direktory uploads).

import paramiko

HOSTNAME = "5.189.154.248"
PORT = "22" 
USERNAME = "heri"
PASSWORD = "Passwd093"


# bikin SSH client dulu
ssh_client = paramiko.SSHClient()

# set policy untuk menambahkan host key yang tidak dikenal
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

sftp_client = None

try:
    # connect ke server SFTP
    ssh_client.connect(hostname=HOSTNAME, port=PORT, username=USERNAME, password=PASSWORD)
    print("✅ connected to ssh_client")

    # buat SFTP client dari SSH client
    sftp_client = ssh_client.open_sftp()
    print("✅ connected to sftp_client")

    #contoh operasi: list file di diretori home
    files = sftp_client.listdir()
    print("Files in home directory:", files)

    files = sftp_client.listdir("uploads")
    print("Files in uploads directory:", files)

    # download file
    path_download = "uploads/sales_data.csv"
    
    local_path = "./sales_data_download_baru.csv"
    sftp_client.get(path_download, local_path)

    
    file_path = "sales_data_download_baru.csv"
    with open(file_path,"r") as file:
        headers = file.readline()
        print("Headers file:", headers)

        headers = headers.strip().split(",")

        lines = file.readlines()
        print("Isi file adalah", lines)

        data_to_write = []
        for line in lines:
            line = line.strip()
            if not line: # skip empty lines
                continue
            print("Line:", line)
            values = line.split(",") # split by comma
            print("Values:", values) # values berisi baris yang menjadi list

            # menambahkan total amount
            total_amount = str(float(values[2])*int(values[3]))
            print("Values setelah ada TotalAmount:", values)

            # join values menjadi satu string
            values = ",".join(values)
            print("Values setelah join:",values)

            # memasukkan values ke dalam list
            data_to_write.append(values)
        print("Valuedata siap disimpan",data_to_write)
    
    # menambahkan header TotalAmount dan join header menjadi satu string
    headers.append("TotalAmount")
    headers = ",".join(headers)

    data_to_write.insert(0,headers)
    print("Data siap disimpan", data_to_write)

    # write data
    file_path = "atika_sales_data_upload.csv"
    with open(file_path,"w") as file:
        for line in data_to_write:
            file.write(line + "\n")

    # upload data
    local_file_path = "./atika_sales_data_upload.csv"
    remote_file_path = "uploads/atika_sales_data.csv"

    sftp_client.put(local_file_path, remote_file_path)
    print(f"✅ File uploaded to {remote_file_path}")

    # cek data sudah terupload
    files = sftp_client.listdir("uploads")
    print("Files in uploads directory:", files)

except paramiko.AuthenticationException:
    print("❌ Authenticaion failed, pleaseverify your credential")
except paramiko.SSHException as ssh_exception:
    print("❌ Unable to establish SSH connection:", ssh_exception)
except FileNotFoundError as fnf_error:
    print("❌ File not found:", fnf_error)
except paramiko.SFTPError as sftp_error:
    print("❌ SFTP erroroccured:", sftp_error)
except Exception as e:
    print("❌ Error:", e)
finally:
    # tutup SFTP cliet jika ada
    if sftp_client:
        sftp_client.close()
    # tutup SSH client
    ssh_client.close()
    print("✅ sftp_client & ssh_client are closed ✨")