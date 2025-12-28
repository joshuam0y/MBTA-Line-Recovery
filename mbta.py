from flask import Flask, jsonify, render_template
import snowflake.connector

app = Flask(__name__)

@app.route('/')
def home():
    return render_template("index.html")

@app.route('/line_recovery')
def line_recovery():
    conn = snowflake.connector.connect(
        user='joshuam0y',
        password='LOL',
        account="rj84036.ca-central-1.aws",
        warehouse='COMPUTE_WH',
        database='MBTA',
        schema='PUBLIC'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM line_recovery ORDER BY line, year;")
    rows = cursor.fetchall()
    cols = [col[0].lower() for col in cursor.description]
    data = [dict(zip(cols, row)) for row in rows]
    conn.close()
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
