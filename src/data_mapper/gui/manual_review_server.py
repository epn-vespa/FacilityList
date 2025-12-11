from flask import Flask, render_template, jsonify, request
import queue

app = Flask(__name__)

running = False
user_choices = queue.Queue()
current_state = {"subject": None,
                 "predicate": None,
                 "object": None,
                 "justification": None,
                 "score": None,
                 "creation_date": None,
                 "creator": None,
                 "list1": None,
                 "list2": None,
                 "total": 0,
                 "current": 0}


def update_state(subject,
                 predicate,
                 object,
                 justification = None,
                 score: float = 0,
                 creation_date: str = None,
                 creator: str = None,
                 list1 = None,
                 list2 = None,
                 total: int = 0,
                 current: int = 0):
    current_state["subject"] = subject
    current_state["predicate"] = predicate
    current_state["object"] = object
    current_state["justification"] = justification
    current_state["score"] = score
    current_state["creation_date"] = creation_date
    current_state["creator"] = creator
    current_state["list1"] = list1
    current_state["list2"] = list2
    current_state["total"] = total
    current_state["current"] = current

def wait_for_user_choice():
    choice_data = user_choices.get()
    return choice_data["choice"], choice_data["justification"]

@app.route("/")
def index():
    return render_template("manual_review.html")


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