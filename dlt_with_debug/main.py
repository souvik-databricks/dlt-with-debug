import re


class dltwithdebug:

    def dlt_avoider(self):
        avoid_cmd = self.cmd[self.cmd.find('def'):]
        avoid_cmd = avoid_cmd.replace("'", '"')

        # replacing dlt.read with the cmd's function
        while True:
            result = re.search('dlt.read\\("(.*)"\\)', avoid_cmd)

            if result:
                avoid_cmd = avoid_cmd.replace(
                    result.group(0), result.group(1) + '()')

            else:
                break

        # replacing LIVE with the cmd function
        while True:
            result = re.search('spark.table\\("LIVE.(.*)"\\)', avoid_cmd)

            if result:
                avoid_cmd = avoid_cmd.replace(
                    result.group(0), result.group(1) + '()')
            else:
                break

        return avoid_cmd

    def __init__(self, cmd, pipeline_id, g):
        self.cmd = cmd
        self.pipeline_id = pipeline_id
        self.g = g

        if self.pipeline_id:
            exec(self.cmd, self.g)
        else:
            self.avoid_cmd = self.dlt_avoider()
            exec(self.avoid_cmd, self.g)
            print(self.avoid_cmd)

