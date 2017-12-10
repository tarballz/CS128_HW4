from django.conf.urls import url
from django.contrib import admin
from kvstore import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^kv-store/get_node_details', views.get_node_details),
    url(r'^kv-store/get_all_replicas', views.get_all_replicas),
    url(r'^kv-store/get_state', views.get_state),
    url(r'^kv-store/update_view$', views.update_view),
    url(r'^kv-store/update_view_receiver', views.update_view_receiver),
    url(r'^kv-store/check_nodes', views.check_nodes),
    url(r'^kv-store/db_broadcast', views.db_broadcast),
    url(r'^kv-store/db_prune', views.db_prune),
    url(r'^kv-store/get_all_partition_ids', views.get_all_partition_ids),
    url(r'^kv-store/get_partition_members', views.get_partition_members),
    url(r'^kv-store/get_entries', views.get_entries),
    url(r'^kv-store/(?P<key>[a-zA-Z0-9_]{1,200})$', views.kvs_response),
    url(r'^kv-store/(?P<key>[a-zA-Z0-9_]{201,})$', views.failure),
]
