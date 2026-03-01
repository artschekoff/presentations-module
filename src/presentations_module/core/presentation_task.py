class PresentationTask:
    def __init__(self, topic: str, language: str, slides_amount: int, grade: str, subject: str, author: str | None = None):
        self.topic = topic
        self.language = language
        self.slides_amount = slides_amount
        self.grade = grade
        self.subject = subject
        self.author = author
