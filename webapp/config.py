import os
import secrets

#CSRF protection is deliberately off, not forgotten.
#
#Every endpoint is a read-only GET: the search form is submitted with
#method="get", there is no session, no login and nothing that mutates state, so
#there is no cross-site request to forge. Turning it on would also break the
#form, because /search binds it to request.args and wtforms validates the
#csrf_token field as part of form.validate(); the token is not in the query
#string, so every search would be rejected.
#
#Turn this on - and move the form to POST - the moment anything here changes
#state, stores a session, or authenticates a user.
WTF_CSRF_ENABLED = False

#Flask signs session cookies and CSRF tokens with this. Nothing here uses
#either yet, but a key committed to a public repository is worthless the moment
#something does, so it comes from the environment.
SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    #Ephemeral, so there is no usable default to leak. Deliberately not a fixed
    #development value: that is how committed secrets come back.
    SECRET_KEY = secrets.token_hex(32)
    print(
        "WARNING: SECRET_KEY is not set, using a random key for this process. "
        "Sessions will not survive a restart and will not be shared between "
        "workers. Set SECRET_KEY in the environment before serving traffic."
    )
