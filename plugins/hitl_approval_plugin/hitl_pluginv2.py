from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from typing import Optional

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>HITL Business Approval</title>
    <style>
        * {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #0f172a;
        }

        .container {
            width: 100%;
            max-width: 480px;
            background: #1e293b;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 25px 60px rgba(0,0,0,0.4);
        }

        .context {
            font-size: 14px;
            color: #cbd5e1;
            margin-bottom: 30px;
            line-height: 1.6;
        }

        .title {
            font-size: 18px;
            font-weight: 600;
            color: #f1f5f9;
            margin-bottom: 20px;
        }

        .action-buttons {
            display: flex;
            gap: 15px;
            margin-bottom: 25px;
        }

        .btn {
            flex: 1;
            padding: 14px;
            border-radius: 14px;
            border: none;
            font-size: 15px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.25s ease;
            color: white;
        }

        .btn-approve {
            background: #2563eb;
        }

        .btn-approve:hover {
            background: #1d4ed8;
        }

        .btn-reject {
            background: #dc2626;
        }

        .btn-reject:hover {
            background: #b91c1c;
        }

        .btn.selected {
            box-shadow: 0 0 0 3px rgba(255,255,255,0.2);
            transform: translateY(-2px);
        }

        .feedback {
            display: none;
            margin-bottom: 20px;
        }

        .feedback.show {
            display: block;
        }

        input {
            width: 100%;
            padding: 14px;
            border-radius: 12px;
            border: 1px solid #334155;
            background: #0f172a;
            color: white;
            font-size: 14px;
        }

        input:focus {
            outline: none;
            border-color: #2563eb;
        }

        .char-counter {
            font-size: 12px;
            text-align: right;
            margin-top: 6px;
            color: #94a3b8;
        }

        .submit-btn {
            width: 100%;
            padding: 16px;
            border-radius: 16px;
            border: none;
            background: #22c55e;
            color: white;
            font-size: 15px;
            font-weight: 600;
            cursor: pointer;
            transition: 0.25s ease;
        }

        .submit-btn:disabled {
            background: #475569;
            cursor: not-allowed;
        }

        .success {
            margin-top: 20px;
            padding: 15px;
            border-radius: 12px;
            background: #16a34a;
            text-align: center;
            font-weight: 600;
            color: white;
        }

        .error {
            margin-top: 20px;
            padding: 15px;
            border-radius: 12px;
            background: #7f1d1d;
            text-align: center;
            font-weight: 600;
            color: white;
        }
        
        .alert {
            padding: 10px;
            margin-top: 10px;
            border-radius: 4px;
            transition: opacity 0.5s ease;
        }
        
        .fade-out {
            opacity: 0;
        }
    </style>
</head>
<body>

<div class="container">

    <div class="context">
        <strong>Task:</strong> <span id="task_id">N/A</span><br>
        <strong>DAG:</strong> <span id="dag_id">N/A</span><br>
        <strong>Run:</strong> <span id="dag_run_id">N/A</span>
    </div>

    <div class="title">Choose Action</div>

    <div class="action-buttons">
        <button class="btn btn-approve" id="approveBtn">Accept</button>
        <button class="btn btn-reject" id="rejectBtn">Reject</button>
    </div>

    <div id="feedbackGroup" class="feedback">
        <input type="text" id="feedback" maxlength="500" placeholder="Enter rejection reason...">
        <div class="char-counter" id="charCounter">0 / 500</div>
    </div>

    <button class="submit-btn" id="submitBtn" disabled>Submit Decision</button>
    <div id="result"></div>

</div>


<script>

let selectedAction = null;

// --- Get query params ONLY ONCE ---
const urlParams = new URLSearchParams(window.location.search);

const taskId = urlParams.get("task_id");
const dagId = urlParams.get("dag_id");
const rawDagRunId = urlParams.get("dag_run_id") || "";
const mapIndex = urlParams.get("map_index") || "-1";
const processingDate = urlParams.get("processing_date");

// --- Fix dag_run_id formatting ---
const cleanDagRunId = rawDagRunId
    .trim()
    .replace(/\s+00:00$/, "+00:00")
    .replace(/\s+/g, "");

// --- Populate header ---
if (taskId) {
    document.getElementById("task_id").textContent = taskId;
}

if (dagId) {
    document.getElementById("dag_id").textContent = dagId;
}

if (rawDagRunId) {
    document.getElementById("dag_run_id").textContent = rawDagRunId;
}

// --- Build API URL ---
const fullApiUrl =
    `/api/v2/dags/${dagId}/dagRuns/${encodeURIComponent(cleanDagRunId)}/taskInstances/${taskId}/${mapIndex}/hitlDetails`;

//const xcomUrl = `/api/v2/dags/${dagId}/dagRuns/${encodeURIComponent(cleanDagRunId)}/taskInstances/${taskId}/xcomEntries`; 

// --- UI Elements ---
const approveBtn = document.getElementById('approveBtn');
const rejectBtn = document.getElementById('rejectBtn');
const feedbackGroup = document.getElementById('feedbackGroup');
const feedback = document.getElementById('feedback');
const submitBtn = document.getElementById('submitBtn');
const charCounter = document.getElementById('charCounter');
const result = document.getElementById('result');

// --- Action selection ---
function selectAction(action) {
    selectedAction = action;

    approveBtn.classList.remove('selected');
    rejectBtn.classList.remove('selected');

    if (action === 'Approve') {
        approveBtn.classList.add('selected');
        feedbackGroup.classList.remove('show');
        feedback.value = '';
    } else {
        rejectBtn.classList.add('selected');
        feedbackGroup.classList.add('show');
    }

    validate();
}

approveBtn.onclick = () => selectAction('Approve');
rejectBtn.onclick = () => selectAction('Reject');

// --- Feedback validation ---
feedback.addEventListener('input', function() {
    charCounter.textContent = `${this.value.length} / 500`;
    validate();
});

function validate() {
    if (!selectedAction) return submitBtn.disabled = true;
    if (selectedAction === 'Reject' && feedback.value.trim().length < 3)
        return submitBtn.disabled = true;

    submitBtn.disabled = false;
}

function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
}

// --- Submit ---
// --- Fixed Submit Handler ---
submitBtn.addEventListener('click', async function() {
    if (!selectedAction) return;

    const payload = {
        chosen_options: [selectedAction],
        params_input: {
            processing_date: processingDate,
            feedback: selectedAction === 'Reject' ? feedback.value.trim() : ""
        }
    };

    try {
        // 1. FIRST: Update HITL details (complete the HITL task)
        const response = await fetch(fullApiUrl, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`HITL API failed: ${response.status}`);
        }

        // 2. ONLY if HITL succeeds, push feedback as XCom (optional)
        //if (selectedAction === 'Reject' && feedback.value.trim()) {
        //    const xcomPayload = {
        //        'key': 'feedback',
        //        'value': feedback.value.trim(),
        //        'map_index': -1
        //    };
            // const csrfToken = getCookie("csrf_token");
        //    const xcomResponse = await fetch(xcomUrl, {
        //        method: 'POST',
        //        credentials: 'include',
        //        headers: { 'Content-Type': 'application/json'},
        //        body: JSON.stringify(xcomPayload)
        //    });

        //    if (!xcomResponse.ok) {
        //        console.warn('XCom feedback failed, but HITL succeeded');
        //    }
        //}

        // Success - HITL task is now complete, downstream will trigger
        result.innerHTML = `<div id="alertBox" class="success">${selectedAction} submitted! Workflow continuing...</div>`;

    } catch (err) {
        console.error('HITL Submission Error:', err);
        result.innerHTML = `<div id="alertBox" class="error">Submission failed: ${err.message}</div>`;
    }

    // Auto-refresh page after success
    setTimeout(() => {
        if (result.querySelector('.success')) {
            window.location.reload();
        } else {
            fadeOutAlert();
        }
    }, 2000);
});

function fadeOutAlert() {
    const alertBox = document.getElementById("alertBox");
    if (alertBox) {
        alertBox.classList.add("fade-out");
        setTimeout(() => result.innerHTML = "", 500);
    }
}

</script>
</body>
</html>
"""

app = FastAPI(title="HITL UI Plugin", version="1.0.0")


@app.get("/hitl-ui", response_class=HTMLResponse)
async def hitl_ui(
        task_id: Optional[str] = Query(None, alias="task_id"),
        dag_id: Optional[str] = Query(None, alias="dag_id"),
        dag_run_id: Optional[str] = Query(None, alias="dag_run_id"),
        processing_date: Optional[str] = Query(None, alias="processing_date")
):
    """
    HITL Approval UI - Pass context via query params (?task_id=foo&dag_id=bar&dag_run_id=baz)
    JS reads from URL params and populates context.
    Use in DAGs: e.g., ExternalTaskSensor or dynamic extra link to f"/custom/hitl-ui?task_id={{task_id}}&..."
    """
    return HTMLResponse(content=HTML_TEMPLATE)


class HitlUIPlugin(AirflowPlugin):
    name = "hitl_ui_plugin"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/custom",
            "name": "HITL UI",
            "tags": ["HITL UI"]
        }
    ]

    external_views = [
        {
            "name": "HITL UI",
            "href": "custom/hitl-ui",
            "destination": "nav",
            "category": "admin",
        }
    ]


