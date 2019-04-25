# CVS Dead Letter Queue Handler

This lambda is used to notify the DevOps team when messages in SQS have fallen into a Dead Letter Queue.

It uses the [cvs-tsk-devops-notify][notify-link] lambda to email the team via GovNotify and uploads a link to the messages to s3.

[notify-link]: https://github.com/dvsa/cvs-tsk-devops-notify

## Quick Start
Run the following:
```bash
pipenv install --dev
pipenv run handler --event example.json
```
