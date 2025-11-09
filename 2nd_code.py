from mrjob.job import MRJob, MRStep
from datetime import datetime
import json

class LogAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_final)
        ]

    def mapper(self, _, line):
        parts = line.strip().split()
        if len(parts) == 3:
            user, timestamp, action = parts
            action = action.lower().strip()
            yield user, json.dumps((timestamp, action))

    def reducer_sum(self, user, values):
        events = []
        for v in values:
            timestamp, action = json.loads(v)
            events.append((timestamp, action))
        events.sort(key=lambda x: x[0])

        total = 0
        login_time = None
        for timestamp, action in events:
            
            t = datetime.fromisoformat(timestamp)
           

            if action == "login" and not login_time:
                login_time = t
            elif action == "logout" and login_time:
                total += (t - login_time).total_seconds()
                login_time = None

        yield None, (user, round(total / 3600, 2))

    def reducer_final(self, _, user_totals):
        all_users = list(user_totals)
        max_time = max(total for _, total in all_users)


        yield "All Users (User : Hours)", ""

    
        for user, total in sorted(all_users, key=lambda x: x[0]):
            yield user, total

        top_users = [user for user, total in all_users if total == max_time]
        for u in top_users:
            yield "Most Active User", f"{u} ({max_time} hours)"

if __name__ == "__main__":
    LogAnalysis.run()
