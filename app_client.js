const zeromq = require('zeromq');
const {upsert_transactions, get_recurring_trans, get_all_trans, delete_database, mongoose} = require("./logic");

// socket to talk to server
console.log("Connecting to app server...");
const requester = zeromq.socket('req');

var x = 0;
requester.on("message", function(reply) {
	var parse_reply = JSON.parse(reply);
	console.log("Received reply", x, ": [", parse_reply, ']');
	x+=1;
});

requester.connect("tcp://localhost:1984");

var transactions = [JSON.stringify({"task": "upsert_transactions", "transactions": [{"trans_id": "1", "user_id": "1", "name": "Brendan Lee 1FJA814AFAF", "amount": 95.24, "date": "1-15-2018"}, {"trans_id": "2", "user_id": "1", "name": "Brendan Lee AJF194L", "amount": 91.45, "date": "1-22-2018"}, {"trans_id": "3", "user_id": "1", "name": "Brendan Lee AJFLJKAJD81", "amount": 98.38, "date": "1-29-2018"}, {"trans_id": "89", "user_id": "1", "name": "Brendan Lee FSJ1415", "amount": 96.20, "date": "2-6-2018"}]}),
					JSON.stringify({"task": "get_recurring_trans"})];
console.log("Sending transactions...");
for (i = 0; i < transactions.length; i++) {
	requester.send(transactions[i]);
}
requester.send("[]");

process.on('SIGINT', function() {
	requester.close();
	process.exit(0);
});