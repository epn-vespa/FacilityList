from flask import Flask, render_template, jsonify, request
import queue

app = Flask(__name__)

running = False
user_choices = queue.Queue()
current_state = {"entity": None,
                 "candidates": [],
                 "total": 0,
                 "current": 0,
                 "list1": None,
                 "list2": None}

def update_state(entity,
                 candidates,
                 total = None,
                 current: int = 0,
                 list1 = None,
                 list2 = None):
    current_state["entity"] = entity
    current_state["candidates"] = candidates
    current_state["total"] = total
    current_state["current"] = current
    current_state["list1"] = list1
    current_state["list2"] = list2

def wait_for_user_choice():
    choice_data = user_choices.get()
    return choice_data["choice"], choice_data["justification"]

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/get_state")
def get_state():
    return jsonify(current_state)


@app.route("/validate", methods=["POST"])
def validate():
    data = request.get_json()
    choice = data.get("choice")
    justification = data.get("justification", "")

    user_choices.put({
        "choice": choice,
        "justification": justification
    })

    return jsonify({"status": "ok"})