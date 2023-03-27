from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from my_task import MyTask #this is our usecase py file
from wtforms import Form, StringField
from airflow.www.app import csrf
# blueprint which will provide the directories from where our plugin will pull files.
bp = Blueprint(
               "test_plugins",
               __name__,
               template_folder="templates", # registers airflow/plugins/templates as a Jinja template folder
               static_folder="static",
               static_url_path="/static/test_plugin",
               )
class FilterForm(Form):
    param1= StringField('param1')
    param2= StringField('param2')
    param3= StringField('param3')
    param4= StringField('param4')

# create a class that inherits AppBuilderView
class TestAppBuilderBaseView(AppBuilderBaseView):
    default_view = "test"
    @expose("/", methods=['GET', 'POST'])
    #this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt # if we don’t want to use csrf
    def test(self):
        form = FilterForm(request.form)
        if request.method == 'POST' and form.validate():
            #Here we are calling our usecase functions
            my_task_output = MyTask(
                   form.param1.data,
                   form.param2.data,
                   form.param3.data,
                   form.param4.data
            )
            df = my_task_output.my_function()
            return df
        return self.render_template("test.html", form = form)
# Now we need to provide name to our plugin
v_appbuilder_view = TestAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Test View", # this is the name of the link displayed
    "category": "Test Plugin", # This is the name of the tab under     which we have our view
    "view": v_appbuilder_view
}
appbuilder_mitem = {
    "name": "Google",
    "href": "https://www.google.com",
    "category": "Search",

}
appbuilder_mitem_toplevel = {
    "name": "Apache",
    "href": "https://www.apache.org/",
}
from airflow.hooks.base import BaseHook

# Will show up in Connections screen in a future version
class PluginHook(BaseHook):
    pass
# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
    pass
# Finally create the Plugin Class

class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    operators = []
    flask_blueprints = [bp]
    # hooks = []
    executors = []
    admin_views = []
    hooks = [PluginHook]
    macros = [plugin_macro]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = [appbuilder_mitem,appbuilder_mitem_toplevel]