import paramiko

class ReadWriteSFTP:
    def __init__(self, host="5.189.154.248", port="22", username="heri", password="Passwd093"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = None
        self.sftp_client = None
    
    def connect_sftp(self):
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # connect ke server SFTP
            self.ssh_client.connect(hostname=self.host, port=self.port, username=self.username, password=self.password)
            print("Connected to ssh_client")

            # buat SFTP client dari SSH client
            self.sftp_client = self.ssh_client.open_sftp()
            print("Connected to sftp_client")

            # list file di direktori home dan upload
            print("Files in home directory:", self.sftp_client.listdir())

            print("Files in uploads directory:", self.sftp_client.listdir("uploads"))

        except paramiko.AuthenticationException:
            print("Authenticaion failed, pleaseverify your credential")
        except paramiko.SSHException as ssh_exception:
            print("Unable to establish SSH connection:", ssh_exception)
        except FileNotFoundError as fnf_error:
            print("File not found:", fnf_error)
        except paramiko.SFTPError as sftp_error:
            print("SFTP erroroccured:", sftp_error)
        except Exception as e:
            print("Error:", e)
    
    def download_file(self, path_download = "uploads/sales_data.csv", local_path = "./sales_data_download_baru.csv"):
        try:
            self.sftp_client.get(path_download, local_path) 

        except Exception as e:
            print("Error when downloading file:", e)

    def transform_file(self,file_path = "sales_data_download_baru.csv", file_path2= "atika_sales_data_upload.csv"):
        try:
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
            with open(file_path2,"w") as file:
                for line in data_to_write:
                    file.write(line + "\n")
        
        except Exception as e:
            print("Error when transforming data:", e)
        
    def upload_file(self, local_file_path = "./atika_sales_data_upload.csv", remote_file_path = "uploads/refactor_atika_sales_data.csv"):
        try:
            self.sftp_client.put(local_file_path, remote_file_path)
            print(f"File uploaded to {remote_file_path}")

            # cek data sudah terupload
            print("Files in uploads directory:", self.sftp_client.listdir("uploads"))
        
        except Exception as e:
            print("Error when uploading data:", e)

    def close_sftp(self):
        # tutup SFTP client jika ada
        if hasattr(self, 'sftp_client') and self.sftp_client:
            self.sftp_client.close()
        if hasattr(self, 'ssh_client') and self.ssh_client:
            self.ssh_client.close()
        print("sftp_client & ssh_client are closed")
    

write_read_sftp = ReadWriteSFTP()

write_read_sftp.connect_sftp()
write_read_sftp.download_file()
write_read_sftp.transform_file()
write_read_sftp.upload_file()
write_read_sftp.close_sftp()

        


