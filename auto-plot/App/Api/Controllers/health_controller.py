from flask_restx import Resource, Namespace

ns = Namespace('health', description='Check the app health')

@ns.route('')
class HealthController(Resource):
    def get(self):
        return {'status': 'ok'}
