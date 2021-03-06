import os
from flask import g, session, Blueprint, current_app, request
from flask import jsonify, abort, json

from . import db, socketio, celery, pdb
from .models import AnomalyStat, AnomalyData, AnomalyStatQuery
# from .models import ExecData, CommData

from sqlalchemy import func, and_

from .tasks import make_async

import pymargo
from pymargo.core import Engine
from pysonata.provider import SonataProvider
from pysonata.client import SonataClient
from pysonata.admin import SonataAdmin

import random

events = Blueprint('events', __name__)


def push_data(data, event='updated_data',  namespace='/events'):
    """Push the data to all connected Socket.IO clients."""
    socketio.emit(event, data, namespace=namespace)


def load_execution_provdb(pid, rid, step):
    """Load execution data from provdb as unqlite file"""

    filtered_records = []
    # collection = pdb.open('anomalies')  # default collection
    jx9_filter = "function($record) { return " \
        "$record.pid == %d && " \
        "$record.rid == %d && " \
        "$record.io_step == %d; } " % (int(pid),
                                       int(rid),  # random.randint(0, 1),
                                       int(step))  # random.randint(0, 8))
    if pdb and pdb.pdb_collections:
        for col in pdb.pdb_collections:
            result = [json.loads(x) for x in col.filter(jx9_filter)]
            filtered_records += result
    print("{} records from provdb".format(
        len(filtered_records)))

    # gpu_count = 0
    # for record in filtered_records:  # reduced_records:
    #     if record['is_gpu_event']:
    #         gpu_count += 1
    # print("...{} are gpu events...".format(gpu_count))

    return filtered_records  # reduced_records


@events.route('/query_executions_pdb', methods=['GET'])
def get_execution_pdb():
    """
    Return a list of execution data within a given time range
    - required:
        min_ts: minimum timestamp
    - options
        max_ts: maximum timestamp
        order: [(asc) | desc]
        pid: program index, default None
        rid: rank index, default None
    """
    pid = request.args.get('pid', None)
    rid = request.args.get('rid', None)
    step = request.args.get('step', None)
    min_ts = request.args.get('min_ts', None)
    max_ts = request.args.get('max_ts', None)
    if all(v is None for v in [pid, rid, step]):
        abort(400)

    print("queried:", pid, rid, step, min_ts, max_ts)

    # parse options
    order = request.args.get('order', 'asc')

    execdata = []
    execdata = load_execution_provdb(pid, rid, step)
    sort_desc = order == 'desc'
    execdata.sort(key=lambda d: d['entry'], reverse=sort_desc)

    return jsonify({"exec": execdata})
    # return jsonify(execdata), 200


@socketio.on('query_stats', namespace='/events')
def query_stats(q):
    nQueries = q.get('nQueries', 5)
    statKind = q.get('statKind', 'stddev')
    ranks = q.get('ranks', [])

    q = AnomalyStatQuery.create({
        'nQueries': nQueries,
        'statKind': statKind,
        'ranks': ranks
    })
    db.session.add(q)
    db.session.commit()


# @events.route('/query_stats', methods=['POST'])
# def post_query_stats():
#     q = request.get_json()
#
#     nQueries = q.get('nQueries', 5)
#     statKind = q.get('statKind', 'stddev')
#     ranks = q.get('ranks', [])
#
#     q = AnomalyStatQuery.create({'nQueries': nQueries,
#                                  'statKind': statKind,
#                                  'ranks': ranks})
#     db.session.add(q)
#     db.session.commit()
#
#     return jsonify({"ok": True})


@socketio.on('connect', namespace='/events')
def events_connect():
    print('socketio.on.connect')


@socketio.on('disconnect', namespace='/events')
def events_disconnect():
    print('socketio.on.disconnect')


# def push_model(model, namespace='/events'):
#     """-------No longer used--------"""
#     """Push the model to all connected Socket.IO clients."""
#     socketio.emit('updated_model', {
#         'class': model.__class__.__name__,
#         'model': model.to_dict()
#     }, namespace=namespace)


# @events.route('/query_history', methods=['POST'])
# def get_history():
#     """-------Legacy design, no longer used--------"""
#     """Return the anomaly history of selected ranks at the step"""
#     q = request.get_json() or {}

#     app = 0
#     ranks = q.get('qRanks', [])
#     step = q.get('last_step', 0)
#     if step is None:
#         step = -1

#     empty_data = {
#         'id': -1,
#         'n_anomalies': 0,
#         'step': step,
#         'min_timestamp': 0,
#         'max_timestamp': 0
#     }
#     step += 1

#     payload = []
#     for rank in ranks:
#         rank = int(rank)

#         stat = AnomalyStat.query.filter(
#             and_(
#                 AnomalyStat.app == app,
#                 AnomalyStat.rank == rank
#             )
#         ).order_by(
#             AnomalyStat.created_at.desc()
#         ).first()

#         if stat is None:
#             payload.append(empty_data)
#             continue

#         data = stat.hist.filter(AnomalyData.step == step).first()
#         if data is None:
#             payload.append(empty_data)
#             continue

#         payload.append(data.to_dict())

#     return jsonify(payload)


# @celery.task
# def push_execution(pid, rid, min_ts, max_ts, order, with_comm):
#     """-------No longer used--------"""
#     """Query execution data and push to socketio clients"""
#     from .wsgi_aux import app
#     with app.app_context():
#         min_ts = int(min_ts)
#         execdata = ExecData.query.filter(ExecData.entry >= min_ts)
#         if max_ts is not None:
#             max_ts = int(max_ts)
#             execdata = execdata.filter(ExecData.exit <= max_ts)

#         if pid is not None:
#             pid = int(pid)
#             execdata = execdata.filter(ExecData.pid == pid)

#         if rid is not None:
#             rid = int(rid)
#             execdata = execdata.filter(ExecData.rid == rid)

#         if order == 'asc':
#             execdata = execdata.order_by(ExecData.entry.asc())
#         else:
#             execdata = execdata.order_by(ExecData.entry.desc())

#         execdata = [d.to_dict(int(with_comm)) for d in execdata.all()]
#         if len(execdata):
#             push_data({
#                 'type': 'execution',
#                 'data': execdata
#             })


# @events.route('/query_executions', methods=['GET'])
# def get_execution():
#     """-------No longer used--------"""
#     """
#     Return a list of execution data within a given time range
#     - required:
#         min_ts: minimum timestamp
#     - options
#         max_ts: maximum timestamp
#         order: [(asc) | desc]
#         with_comm: 1 or (0)
#         pid: program index, default None
#         rid: rank index, default None
#     """
#     min_ts = request.args.get('min_ts', None)
#     if min_ts is None:
#         abort(400)

#     # parse options
#     max_ts = request.args.get('max_ts', None)
#     order = request.args.get('order', 'asc')
#     with_comm = request.args.get('with_comm', 0)
#     pid = request.args.get('pid', None)
#     rid = request.args.get('rid', None)

#     push_execution.delay(pid, rid, min_ts, max_ts, order, with_comm)
#     return jsonify({}), 200


# def load_execution_db(pid, rid, min_ts, max_ts, order, with_comm):
#     """-------No longer used--------"""
#     """Query execution data from db"""
#     min_ts = int(min_ts)
#     execdata = ExecData.query.filter(ExecData.entry >= min_ts)
#     if max_ts is not None:
#         max_ts = int(max_ts)
#         execdata = execdata.filter(ExecData.exit <= max_ts)

#     if pid is not None:
#         pid = int(pid)
#         execdata = execdata.filter(ExecData.pid == pid)

#     if rid is not None:
#         rid = int(rid)
#         execdata = execdata.filter(ExecData.rid == rid)

#     if order == 'asc':
#         execdata = execdata.order_by(ExecData.entry.asc())
#     else:
#         execdata = execdata.order_by(ExecData.entry.desc())

#     execdata = [d.to_dict(int(with_comm)) for d in execdata.all()]
#     return execdata


# def load_execution_file(pid, rid, step, order, with_comm):
#     """-------No longer used--------"""
#     """Load execution data from json file"""
#     path = current_app.config['EXECUTION_PATH']
#     if path is None:
#         return []

#     path = os.path.join(
#         path,
#         '{}'.format(pid),
#         '{}'.format(rid),
#         '{}.json'.format(step))

#     if not os.path.exists(path) or not os.path.isfile(path):
#         return []

#     with open(path) as f:
#         data = json.load(f)

#     if data is None or not isinstance(data, dict):
#         return []

#     return data.get('exec', []), data.get('comm', [])


# @celery.task
# def update_execution_db(execdata, commdata):
#     """-------No longer used--------"""
#     from .wsgi_aux import app
#     with app.app_context():
#         if execdata is not None:
#             db.engine.execute(ExecData.__table__.insert(), execdata)

#         if commdata is not None:
#             db.engine.execute(CommData.__table__.insert(), commdata)
