from flask import Flask
from flask import render_template
from dashboard.dashapp import init_dashboard

app = Flask(__name__, instance_relative_config=False)
app = init_dashboard(app)

@app.route("/")
def home():

    return render_template(
        "index.html",
        title="BDA Project Main Page",
        
    )

if __name__ == '__main__':
    app.run()
