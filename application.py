import logging

from flask import Flask, render_template, request, jsonify, session, flash, redirect
from core.aws import *
from core.ddb import scan_items, delete_item
from core.reporting import *
from core.config import get_region_friendlyname

application = Flask(__name__)
application.secret_key = "H3%GNalCn11B^Q2a9Lccgy*s0"


@application.before_request
def before_request():
    if request.url.startswith('http://'):
        url = request.url.replace('http://', 'https://', 1)
        code = 301
        return redirect(url, code=code)


""" ----------------------------------------- Login ----------------------------------------- """


@application.route('/login', methods=['POST'])
def login():
    if request.form['password'] == 'alphaquebec10' and request.form['username'] == 'labmin':
        session['logged_in'] = True
    else:
        flash('wrong password!')
    return redirect('/')


@application.route("/logout")
def logout():
    session['logged_in'] = False
    return root()


""" ----------------------------------------- Pages ----------------------------------------- """


@application.route('/')
def root():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        if test_db_connection()[1] == 1:
            return render_template('labs.html', my_list=scan_items(), activelabs=active_labs())
        else:
            return redirect('/settings')


@application.route('/oldlabs')
def oldlabs():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        if test_db_connection()[1] == 1:
            return render_template('oldlabs.html', my_list=scan_items())
        else:
            return redirect('/settings')


@application.route('/settings')
def settings():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return render_template('settings.html',
                               initialised=get_config_item('initialised'),
                               default_region=get_region_friendlyname(get_config_item('default_region'))
                               )


@application.route('/reports')
def reports():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        if test_db_connection()[1] == 1:
            return render_template('reports.html', report_data=report_all())
        else:
            return redirect('/settings')


@application.route('/theme')
def theme():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return render_template('theme.html')


""" ----------------------------------------- Triggers ----------------------------------------- """


@application.route('/cfcreate', methods=['GET', 'POST'])
def cfcreate():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
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
def cfdelete():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({
            'response':    delete_cf_stack(
                request.args.get('stackname'),
                request.args.get('region'),
                request.args.get('stackid'),
                request.args.get('starttime'),
                request.args.get('instancesize')
            )})


@application.route('/addtime', methods=['GET', 'POST'])
def addtime():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
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
def setips():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return update_running_lab_ips()


@application.route('/updatelabstatus', methods=['POST'])
def updatelabstatus():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return update_global_lab_status()


@application.route('/createkey', methods=['GET', 'POST'])
def createkey():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({
                'key': create_key_pair(
                    request.args.get('region')
                )})


@application.route('/ec2price', methods=['GET', 'POST'])
def ec2price():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        if request.method == 'GET':
            return jsonify({
                'cost': get_ec2_price(
                    request.args.get('instancesize'),
                    request.args.get('region'),
                    request.args.get('ttl'),
                    request.args.get('labno')
                )})


@application.route('/keypairs', methods=['GET', 'POST'])
def keypairs():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        if request.method == 'GET':
            return jsonify({
                'keypairs': list_keypairs(request.args.get('region'))})


@application.route('/updatecreds', methods=['POST'])
def updatecreds():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        update_credentials(request.form['key'], request.form['secretkey'])
        return "Done"


@application.route('/testconnection', methods=['GET', 'POST'])
def testconnection():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': test_aws_connection()})


@application.route('/deletedbentry', methods=['GET', 'POST'])
def deletedbentry():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': delete_item(request.args.get('stackid'))})


@application.route('/updatedefaultregion', methods=['GET', 'POST'])
def updatedefaultregion():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': update_config_item('default_region', str(request.args.get('defaultregion')))})


@application.route('/initialConfig', methods=['GET', 'POST'])
def initialConfig():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': initial_config(request.args.get('s3bucket'))})


@application.route('/copytos3', methods=['GET', 'POST'])
def copytos3():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': create_s3_documents()})


@application.route('/updatePrices', methods=['GET', 'POST'])
def updatePrices():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': get_ec2_pricelists()})


@application.route('/cheapestregion', methods=['GET', 'POST'])
def cheapestregion():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return jsonify({'result': get_ec2_cheapest_regions(request.args.get('instance'))})


""" ----------------------------------------- Error Handling ----------------------------------------- """


@application.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500

# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()
