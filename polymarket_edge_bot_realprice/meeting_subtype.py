def infer_meeting_subtype(question, catalyst_type=None):
    if str(catalyst_type or "") != "call_or_meeting":
        return None

    text = str(question or "").lower()
    if not text:
        return "meeting_generic"

    if "resume" in text and ("talk" in text or "negotiat" in text):
        return "resume_talks"

    talk_patterns = (
        "talk to",
        "talk with",
        "talks with",
        "phone call",
        "call with",
        "speak with",
        "speak to",
    )
    if any(pattern in text for pattern in talk_patterns):
        return "talk_call"

    meeting_patterns = (
        "meet with",
        "meeting with",
        "meet ",
        "meeting ",
    )
    if any(pattern in text for pattern in meeting_patterns):
        return "meeting"

    if "talk" in text or "call" in text or "speak" in text:
        return "talk_call"
    if "meet" in text or "meeting" in text:
        return "meeting"
    return "meeting_generic"


def meeting_subtype_label(subtype):
    labels = {
        "talk_call": "Talk / call setup",
        "meeting": "Meeting setup",
        "resume_talks": "Talks resume",
        "meeting_generic": "Call or meeting",
    }
    return labels.get(str(subtype or ""), "Call or meeting")
