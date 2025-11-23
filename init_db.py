import psycopg2
try:
    conn = psycopg2.connect(
        host="localhost",
        database="smartcity_db",
        user="db_username",
        password="db_sifreniz",
        port="5432"
    )
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS traffic_stats (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        city VARCHAR(50),
        total_vehicles INT,
        avg_speed DOUBLE PRECISION,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cur.execute(create_table_query)
    conn.commit()
    print("Tablo başarıyla oluşturuldu: traffic_stats")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Hata: {e}")