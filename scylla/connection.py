from ._uri import URI
from .request import Methods
from .response import Response, Statuses


class Connection(object):
    @property
    def url(self):
        return self._url

    def __init__(self, url):
        if isinstance(url, basestring):
            url = URI(url)
        self._url = url

    def to_json(self):
        result = {'url': str(self.url)}
        return result

    def __call__(self, request):
        response = None

        if request.method == Methods.POST:
            response = self._post(request)
        elif request.method == Methods.GET:
            response = self._get(request)
        elif request.method == Methods.PUT:
            response = self._put(request)
        elif request.method == Methods.DELETE:
            response = self._delete(request)

        return response

    def _post(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response

    def _get(self, request):
        response = Response(request.client, Statuses.OK, data=self.to_json())
        return response

    def _put(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response

    def _delete(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response


