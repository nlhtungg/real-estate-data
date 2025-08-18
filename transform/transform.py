from utils.spark_config import create_spark_session
from utils.data_processor import read_raw_data
from utils.data_saver import save_to_cleaned_bucket

def main():
    print("Khởi tạo Spark session...")
    spark = create_spark_session()
    
    try:
        print("Đọc dữ liệu từ bucket raw...")
        df = read_raw_data(spark)
        
        if df is not None:
            print("Đọc dữ liệu thành công!")
            # Example usage of save_to_cleaned_bucket
            save_to_cleaned_bucket(df, "example_table", spark)
        else:
            print("Không thể đọc dữ liệu!")
            
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        print("Dừng Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()
