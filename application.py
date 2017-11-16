import base64

from flask import Flask, render_template, request, jsonify, session, flash, redirect
from werkzeug.contrib.fixers import ProxyFix
import flask_login
import logging, logging.config, yaml

from core.aws import *
from core.rekognition import *
from core.ddb import scan_items, delete_item
from core.reporting import *
from core.config import get_region_friendlyname

application = Flask(__name__)

# Proxy fix so that x-forwarded-proto works for SSL redirect
application.wsgi_app = ProxyFix(application.wsgi_app)

# Logging
logging.config.dictConfig(yaml.load(open('logging.yaml')))
logfile = logging.getLogger('file')
#logconsole = logging.getLogger('console')
logfile.debug("Debug FILE")
#logconsole.debug("Debug CONSOLE")

# Login Management
login_manager = flask_login.LoginManager()
login_manager.init_app(application)
application.secret_key = "H3%GNalCn11B^Q2a9Lccgy*s0"
users = {'labmin': {'password': 'secret'}}


class User(flask_login.UserMixin):
    pass


@login_manager.user_loader
def user_loader(email):
    if email not in users:
        return

    user = User()
    user.id = email
    return user


@application.before_request
def before_request():
    if "127.0.0.1" not in request.host:
        x_forwarded_proto = request.headers.get('X-Forwarded-Proto')
        if x_forwarded_proto == 'http':
            url = request.url.replace('http://', 'https://', 1)
            return redirect(url)


""" ----------------------------------------- Login ----------------------------------------- """


@application.route('/')
def root():
    return render_template('login.html')


@application.route('/login',  methods=['GET', 'POST'])
def login():
    if request.form['username'] in users:
        if request.form['password'] == users[request.form['username']]['password']:
            user = User()
            user.id = request.form['username']
            flask_login.login_user(user)
            return redirect('/launch')
    return redirect('/')


@application.route('/logout')
def logout():
    flask_login.logout_user()
    return redirect('/')


""" ------------------------------------------ Pages ------------------------------------------ """


@application.route('/launch')
@flask_login.login_required
def launch():
    if test_db_connection()[1] == 1:
        return render_template('labs.html', my_list=scan_items(), activelabs=active_labs())
    else:
        return redirect('/settings')


@application.route('/oldlabs')
@flask_login.login_required
def oldlabs():
    if test_db_connection()[1] == 1:
        return render_template('oldlabs.html', my_list=scan_items())
    else:
        return redirect('/settings')


@application.route('/settings')
@flask_login.login_required
def settings():
    return render_template('settings.html',
                           initialised=get_config_item('initialised'),
                           default_region=get_region_friendlyname(get_config_item('default_region'))
                           )


@application.route('/polly')
@flask_login.login_required
def whatsthat():
    return render_template('polly.html',
                           voices=polly.listVoices())


@application.route('/rekognition')
@flask_login.login_required
def rekognition():
    return render_template('rekognition.html',
                           voices=polly.listVoices())


@application.route('/reports')
@flask_login.login_required
def reports():
    if test_db_connection()[1] == 1:
        return render_template('reports.html', report_data=report_all())
    else:
        return redirect('/settings')


@application.route('/theme')
@flask_login.login_required
def theme():
    return render_template('theme.html')


""" ----------------------------------------- Triggers ----------------------------------------- """


@application.route('/cfcreate', methods=['GET', 'POST'])
@flask_login.login_required
def cfcreate():
    return jsonify({
        'response': create_cf_stack(
            request.args.get('stackname'),
            request.args.get('region'),
            request.args.get('instance'),
            request.args.get('keypair'),
            request.args.get('userpassword'),
            request.args.get('ttl'),
            request.args.get('cost'),
            request.args.get('labno')
        )})


@application.route('/cfdelete', methods=['GET', 'POST'])
@flask_login.login_required
def cfdelete():
    return jsonify({
        'response':    delete_cf_stack(
            request.args.get('stackname'),
            request.args.get('region'),
            request.args.get('stackid'),
            request.args.get('starttime'),
            request.args.get('instancesize')
        )})


@application.route('/addtime', methods=['GET', 'POST'])
@flask_login.login_required
def addtime():
    return jsonify({
        'response': update_instance_endtime(
            request.args.get('stackname'),
            request.args.get('region'),
            request.args.get('stackid'),
            request.args.get('add_mins'),
            request.args.get('instancesize'),
            request.args.get('cost')
        )})


@application.route('/setips', methods=['POST'])
@flask_login.login_required
def setips():
    return update_running_lab_ips()


@application.route('/updatelabstatus', methods=['POST'])
@flask_login.login_required
def updatelabstatus():
    return update_global_lab_status()


@application.route('/createkey', methods=['GET', 'POST'])
@flask_login.login_required
def createkey():
    return jsonify({
            'key': create_key_pair(
                request.args.get('region')
            )})


@application.route('/ec2price', methods=['GET', 'POST'])
@flask_login.login_required
def ec2price():
    if request.method == 'GET':
        return jsonify({
            'cost': get_ec2_price(
                request.args.get('instancesize'),
                request.args.get('region'),
                request.args.get('ttl'),
                request.args.get('labno')
            )})


@application.route('/keypairs', methods=['GET', 'POST'])
@flask_login.login_required
def keypairs():
    if request.method == 'GET':
        return jsonify({
            'keypairs': list_keypairs(request.args.get('region'))})


@application.route('/updatecreds', methods=['POST'])
@flask_login.login_required
def updatecreds():
    update_credentials(request.form['key'], request.form['secretkey'])
    return "Done"


@application.route('/testconnection', methods=['GET', 'POST'])
@flask_login.login_required
def testconnection():
    return jsonify({'result': test_aws_connection()})


@application.route('/deletedbentry', methods=['GET', 'POST'])
@flask_login.login_required
def deletedbentry():
    return jsonify({'result': delete_item(request.args.get('stackid'))})


@application.route('/updatedefaultregion', methods=['GET', 'POST'])
@flask_login.login_required
def updatedefaultregion():
    return jsonify({'result': update_config_item('default_region', str(request.args.get('defaultregion')))})


@application.route('/initialConfig', methods=['GET', 'POST'])
@flask_login.login_required
def initialConfig():
    return jsonify({'result': initial_config(request.args.get('s3bucket'))})


@application.route('/copytos3', methods=['GET', 'POST'])
@flask_login.login_required
def copytos3():
    return jsonify({'result': create_s3_documents()})


@application.route('/updatePrices', methods=['GET', 'POST'])
@flask_login.login_required
def updatePrices():
    return jsonify({'result': get_ec2_pricelists()})


@application.route('/cheapestregion', methods=['GET', 'POST'])
@flask_login.login_required
def cheapestregion():
    return jsonify({'result': get_ec2_cheapest_regions(request.args.get('instance'))})


""" ----------------------------------------- ToolKits ----------------------------------------- """


@application.route('/pollytalk', methods=['GET', 'POST'])
def pollytalk():
    url = jsonify({'result': polly.toS3(
        request.args.get('pollyinput'),
        request.args.get('voice')
    )})
    return url


@application.route('/pollyvoices', methods=['GET', 'POST'])
def pollyvoices():
    url = jsonify({'result': polly.listVoices()})
    return url


@application.route('/rekognise', methods=['GET', 'POST'])
def rekognise():
    image = request.args.get('image')
    voice = request.args.get('voice')

    # Convert received content to utf-8
    content = image.split(';')[1]
    image_encoded = content.split(',')[1]
    body = base64.decodebytes(image_encoded.encode('utf-8'))

    # Send to Rekognition
    url = jsonify({'result': rekog.detectObject(body, voice)})
    return url


""" ----------------------------------------- Error Handling ----------------------------------------- """


@application.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500


@login_manager.unauthorized_handler
def unauthorized_handler():
    return redirect('/')


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()
