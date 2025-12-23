"""
Health controller for the API.

Provides a simple health check endpoint returning status OK.

.. module:: App.Api.Controllers.health_controller
"""

from flask_restx import Resource, Namespace

ns = Namespace('health', description='Check the app health')

@ns.route('')
class HealthController(Resource):
    """Health check resource that returns a basic status."""
    def get(self):
        """Return a simple JSON health-status payload.

        :returns: Dictionary with service status information.
        :rtype: dict
        """
        return {'status': 'ok'}
