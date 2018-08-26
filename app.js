const program = require("commander");
const inquirer = require("inquirer");
const zeromq = require('zeromq');
const mongoXlsx = require('mongo-xlsx');
const {upsert_transactions, get_recurring_trans, get_all_trans, getMostRecentDate, delete_database, mongoose} = require("./logic");
const url = "mongodb://localhost:27017/interview_challenge";

/* For regular command-line usage (transactions_array input must be JSON-formatted)
   Uncomment the TCP communication for safe use */

program
	.command("upsert_transactions <transactions_array>")
	.description("Add transactions.")
	.action((transactions_array) => {
		upsert_transactions(JSON.parse(transactions_array));
	});

program
	.command("get_recurring_trans")
	.description("Get recurring transaction groups.")
	.action(() => {
		get_recurring_trans();
	});

program
	.command("delete_database")
	.description("Delete database.")
	.action(() => {
		delete_database();
		console.log("Deleted database.");
	});

program.parse(process.argv);

/* For interactive command-line user-interface prompt usage */

var APIprompt = {
	type: "list",
	name: "transaction_function",
	message: "Which API call would you like to pick?",
	choices: ["upsert_transactions", "get_recurring_trans", "get_all_trans", "get_excel_recurring_trans", "getMostRecentDate", "delete_database"]
};

var transactionAddPrompt = [
	{
		type: "input",
		name: "trans_id",
		message: "Input trans_id of transaction."
	},
	{
		type: "input",
		name: "user_id",
		message: "Input user_id of transaction."
	},
	{
		type: "input",
		name: "name",
		message: "Input name of transaction."
	},
	{
		type: "input",
		name: "amount",
		message: "Input amount of transaction."
	},
	{
		type: "input",
		name: "date",
		message: "Input date of transaction."
	},
];

var transactionAddAnother = {
	type: "list",
	name: "another_transaction",
	message: "Add another transaction?",
	choices: ["No", "Yes"]
};

function selectAPI() {
	inquirer.prompt(APIprompt).then(answers => {
		if (answers.transaction_function == "upsert_transactions") {
			transactionAdd();
		} else if (answers.transaction_function == "get_recurring_trans") {
			get_recurring_trans();
		} else if (answers.transaction_function == "get_all_trans") {
			get_all_trans();
		} else if (answers.transaction_function == "get_excel_recurring_trans") {
			var data = [];
			var model = mongoXlsx.buildDynamicModel(data);
			mongoXlsx.xlsx2MongoData("./sample_transactions.xlsx", model, function(err, mongoData) {
				upsert_transactions(mongoData); 
			});
		} else if (answers.transaction_function == "getMostRecentDate") {
			getMostRecentDate();
		}
		else if (answers.transaction_function == "delete_database") {
			console.log("Deleting database.")
			delete_database();
		}
	});
}

var transactionsArray = [];

function transactionAdd() {
	inquirer.prompt(transactionAddPrompt).then(answers => {
		transactionsArray.push(answers);
		inquirer.prompt(transactionAddAnother).then(answers => {
			if (answers.another_transaction == "Yes") {
				console.log("Adding another transaction.");
				transactionAdd();
			} else if (answers.another_transaction == "No") {
				upsert_transactions(transactionsArray);
			}
		});
	});
}

/* Uncomment this part and comment-out the server code below in order to run interactive command-line user-interface prompt */
// if (process.argv[2] == null) {
// 	selectAPI();
// }

/* For TCP communication between server and client */

// socket to talk to clients
const responder = zeromq.socket('rep');

var timer;

responder.on("message", function(request) {
	clearTimeout(timer);
	var parse_request = JSON.parse(request);
	console.log("Received request: [", parse_request, "]");
	if (parse_request.task == "upsert_transactions") {
		// send upsert_transactions reply back to client.
		console.log("Sending upsert_transactions output...");
		var transactions = parse_request.transactions;
		upsert_transactions(transactions).then(function(result) {
			mongoose.connect(url, {useNewUrlParser: true});
			var transactions_reply = JSON.stringify(result);
			responder.send(transactions_reply);
		});
	} else if (parse_request.task == "get_recurring_trans") {
		// send get_recurring_trans reply back to client
		console.log("Sending get_recurring_trans output...");
		get_recurring_trans().then(function(result) {
			mongoose.connect(url, {useNewUrlParser: true});
			var transactions_reply = JSON.stringify(result);
			responder.send(transactions_reply);
		});
	}
	timer = setTimeout(function() {
		var timeout = '["Timeout after 10 seconds."]';
		console.log(timeout);
		responder.send(timeout);
	}, 10000);
});

responder.bind('tcp://*:1984', function(err) {
	if (err) {
	console.log(err);
	} else {
	console.log("Listening on TCP port 1984...");
	}
});

process.on('SIGINT', function() {
	responder.close();
	process.exit(0);
});