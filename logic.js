const mongoose = require("mongoose");
const async = require("async");
const kmeans = require('ml-kmeans');
const assert = require("assert");
const url = "mongodb://localhost:27017/interview_challenge";
mongoose.Promise = global.Promise;

mongoose.connect(url, {useNewUrlParser: true});

/* ml-kmeans package example: https://www.npmjs.com/package/ml-kmeans 
(centers are optional) */

// let data = [[1, 1, 1], [1, 2, 1], [-1, -1, -1], [-1, -1, -1.5]];
// let centers = [[1, 2, 1], [-1, -1, -1]];
 
// let ans = kmeans(data, 2, { initialization: centers });
// console.log(ans);

/*
KMeansResult {
  clusters: [ 0, 0, 1, 1 ],
  centroids: 
   [ { centroid: [ 1, 1.5, 1 ], error: 0.25, size: 2 },
     { centroid: [ -1, -1, -1.25 ], error: 0.0625, size: 2 } ],
  converged: true,
  iterations: 1
}
*/

const transactionSchema = mongoose.Schema({
	// Schema for upsert_transactions() input
	trans_id: {type: String},
	user_id: {type: String},
	name: {type: String},
	base_name: {type: String},
	amount: {type: Number},
	date: {type: Date},
	is_recurring: {type: Boolean}
});

const getRecurringTransactionSchema = mongoose.Schema({
	// Schema for recurring transactions output of upsert_transactions() and get_recurring_trans()
	name: {type: String},
	base_name: {type: String},
	user_id: {type: String},
	next_amt: {type: Number},
	next_date: {type: Date},
	transactions: {type: Array},
	trans_ids: {type: Array}
})

const mostRecentDateSchema = mongoose.Schema({
	// Schema for storing most recent date (out of all transactions, not just one transaction group)
	most_recent_date: {type: Date}
});

const Transaction = mongoose.model("Transaction", transactionSchema);

const getRecurringTransaction = mongoose.model("getRecurringTransaction", getRecurringTransactionSchema);

const mostRecentDate = mongoose.model("mostRecentDate", mostRecentDateSchema);

var output_array = [];

function upsert_transactions(transactions) {
	return new Promise(function(resolve, reject) {
		Transaction.create(transactions, (err) => {
			if (err) throw err;
			/* Assumption: All trans_ids are different, regardless of user */
			// console.log(transactions);
			console.log("Added new transaction(s) to database.");

			async.eachSeries(transactions, function(transaction, callback0) {
				/* Use regular expression to remove last word (reference tag) of string and get base company name */
				/* Assumption: reference tag contains integer 0-9 */
				const re = / (?=.*\d)\w+$/;
				const get_base_name = transaction.name.replace(re, '');
				Transaction.updateOne({trans_id: transaction.trans_id}, {base_name: get_base_name}, function(err, update) {
					if (err) throw err;
					mostRecentDate.findOne({}, function(err, date_doc) {
						// Store most recent date out of all transactions
						if (err) throw err;
						var transaction_date = new Date(transaction.date);
						if (date_doc == null) {
							mostRecentDate.create({most_recent_date: transaction_date});
							callback0();
						} else if (transaction_date.getTime() > date_doc.most_recent_date.getTime()) {
							mostRecentDate.updateOne(date_doc, {most_recent_date: transaction_date}, function(err, update) {
								if (err) throw err;
								callback0();
							});
						} else {
							callback0();
						}
					});
				});
			}, function(err) {
				if (err) throw err;
				Transaction.aggregate(
					/* Sort by date and get count >= 3 because recurring transaction group must have at least 3 transactions */
					[{$sort: {date: 1}},
					{$group: {
						_id: {base_name: "$base_name", user_id: "$user_id"},
						count: {$sum: 1},
						trans_ids: {$push: "$trans_id"},
						dates: {$push: "$date"},
						amounts: {$push: "$amount"}},
					},
					{$match: {count: {$gte: 3}}}], function(err, result) {

					if (err) throw err;
					console.log(result);
					async.eachSeries(result, function(result_elem, callback1) {
						async.waterfall([
							function(callback) {
								const kmeans_calc = calculateK_Means(result_elem);
								const kmeans_result = kmeans_calc.kmeans_result;
								const k = kmeans_calc.k;
								callback(null, kmeans_result, k);
							},
							function(kmeans_result, k, callback) {
								const kmeans_trans_id_groups = find_transID_kmean_clusters(kmeans_result, k);
								callback(null, kmeans_trans_id_groups);
							},
							function(kmeans_trans_id_groups, callback) {
								/* Make recurring transaction groups output */
								async.eachSeries(kmeans_trans_id_groups, function(kmeans_trans_id_group, callback2) {
									const recurring_trans_array = [];
									const recurring_trans_ids = [];
									async.eachSeries(kmeans_trans_id_group, function(kmeans_trans_id, callback3) {
										if (kmeans_trans_id_group.length >= 3) {
											Transaction.findOne({trans_id: kmeans_trans_id.trans_id}, function(err, found_doc) {
												if (err) throw err;
												if (found_doc == null) {
													callback3();
												} else {
													recurring_trans_array.push(found_doc);
													recurring_trans_ids.push(kmeans_trans_id.trans_id);
													callback3();
												}
											});
										} else {
											callback3();
										}
									}, function(err) {
										if (err) throw err;
										if (recurring_trans_array.length > 0) {
											const most_recent_trans = recurring_trans_array[recurring_trans_array.length - 1];
											/* For next date, take average of 2 most recent intervals and add to most recent date */
											const latest_time_interval_millisec =
												Math.ceil(((recurring_trans_array[recurring_trans_array.length - 1].date.getTime()
												- recurring_trans_array[recurring_trans_array.length - 2].date.getTime())
												+ (recurring_trans_array[recurring_trans_array.length - 2].date.getTime() 
												- recurring_trans_array[recurring_trans_array.length - 3].date.getTime())) / 2);
											const latest_time_interval_days = Math.ceil(latest_time_interval_millisec / (1000 * 3600 * 24));
											const output_entry = {
												name: most_recent_trans.name,
												base_name: most_recent_trans.base_name,
												user_id: most_recent_trans.user_id,
												next_amt: most_recent_trans.amount,
												next_date: most_recent_trans.date.setDate(most_recent_trans.date.getDate() + latest_time_interval_days),
												transactions: recurring_trans_array,
												trans_ids: recurring_trans_ids
											}
											output_array.push(output_entry)
											callback2();
										} else {
											callback2();
										}
									});
								}, function(err) {
									if (err) throw err;
									callback();
								});
						}], function(err) {
							if (err) throw err;
							callback1();
						});
					}, function(err) {
						if (err) throw err;
						resolve(sendRecurringTransToDatabase(output_array));
					});
				});
			});
		});
	});
};

function calculateK_Means(result_elem) {
	const amountArray = [];
	var amountSum = 0;
	for (i = 0; i < result_elem.amounts.length; i++) {
		amountArray.push([result_elem.amounts[i], result_elem.dates[i].getDate()]);
		amountSum += result_elem.amounts[i];
	}
	var amountAverage = amountSum / result_elem.amounts.length;
	// console.log(amountArray);
	// console.log(amountAverage);
	var best_k = 0;
	var kmeans_result;
	for (k = 1; k <= amountArray.length; k++) {
		// Use personal heuristic variant of elbow method to hopefully find best k
		// Boundaries can be adjusted, depending on dataset
		kmeans_result = kmeans(amountArray, k);
		var error_sum = 0;
		for (i = 0; i < kmeans_result.centroids.length; i++) {
			error_sum += kmeans_result.centroids[i].error;
		}
		// console.log(error_sum);
		if (Math.abs(amountAverage) <= 100) {
			if (error_sum <= 300) {
				best_k = k;
				break;
			}
		} else if (Math.abs(amountAverage) <= 500) {
			if (error_sum <= 1500) {
				best_k = k;
				break;
			}
		}
		else if (Math.abs(amountAverage) <= 1000) {
			if (error_sum <= 50000) {
				best_k = k;
				break;
			}
		}
		else if (Math.abs(amountAverage) <= 10000) {
			if (error_sum <= 300000) {
				best_k = k;
				break;
			}
		} else if (Math.abs(amountAverage) > 10000) {
			if (error_sum <= 1000000) {
				best_k = k;
				break;
			}
		}
	}
	kmeans_result["base_name"] = result_elem._id.base_name;
	kmeans_result["trans_ids"] = result_elem.trans_ids;
	console.log(kmeans_result);
	return {kmeans_result: kmeans_result, k: best_k};
}

function find_transID_kmean_clusters(kmeans_result, k) {
	const kmeans_trans_id_groups = [];
	for (i = 0; i < k; i++) {
		var group_cluster = [];
		for (j = 0; j < kmeans_result.clusters.length; j++) {
			if (kmeans_result.clusters[j] == i) {
				var trans_id = kmeans_result.trans_ids[j]
				group_cluster.push({trans_id});
			}
		}
		kmeans_trans_id_groups.push(group_cluster);
	}
	// console.log(kmeans_trans_id_groups);
	return kmeans_trans_id_groups;
}

function sendRecurringTransToDatabase(output_array) {
	return new Promise(function(resolve, reject) {
		async.eachSeries(output_array, function(output_array_elem, callback4) {
			/* If match = false, then recurring transaction group doesn't exist, so create it */
			/* If match = true, then recurring transaction group already exists, so replace and update */
			var match = false;
			getRecurringTransaction.find({base_name: output_array_elem.base_name}, function(err, found_docs) {
				if (err) throw err;
				async.eachSeries(found_docs, function(found_doc, callback5) {
					let found = output_array_elem.trans_ids.some(trans_id => found_doc.trans_ids.includes(trans_id));
					mostRecentDate.findOne({}, function(err, date_doc) {
						if (found_doc.next_date.getTime() < date_doc.most_recent_date.getTime() - (7 * 86400000)) {
							// Remove recurring transaction group if too much time has passed (7 days) since next expected date
							getRecurringTransaction.deleteOne({_id: found_doc._id}, function(err) {
								if (err) throw err;
								callback5();
							})
						} else {
							if (found) {
								getRecurringTransaction.replaceOne({_id: found_doc._id}, output_array_elem, function(err) {
									if (err) throw err;
									match = true;
									callback5();
								});
							} else {
								callback5();
							}
						}
					});
				}, function(err) {
					if (err) throw err;
					mostRecentDate.findOne({}, function(err, date_doc) {
						var output_elem_date = new Date(output_array_elem.next_date);
						if (output_elem_date.getTime() < date_doc.most_recent_date.getTime() - (7 * 86400000)) {
							// Do not create recurring transaction group if too much time (7 days) has passed since expected next date
							callback4();
						} else {
							if (match == false) {
								getRecurringTransaction.create(output_array_elem, (err) => {
									if (err) throw err;
									callback4();
								});
							} else {
								callback4();
							};
						}
					});
				});
			});
		}, function(err) {
			if (err) throw err;
			resolve(get_recurring_trans());
		});
	});
}

function get_recurring_trans() {
	/* Return output with desired fields in alphabetical order */
	return new Promise(function(resolve, reject) {
		getRecurringTransaction.aggregate(
			[{$sort: {name: 1}}], function(err, recurring_transactions) {

			if (err) throw err;
			console.log(recurring_transactions);
			mongoose.disconnect();
			resolve(recurring_transactions);
		});
	});
};

const get_all_trans = () => {
	return new Promise(function(resolve, reject) {
		Transaction.aggregate([{$sort: {name: 1}}], function(err , all_transactions) {
			if (err) throw err;
			console.log(all_transactions);
			mongoose.disconnect();
			resolve(all_transactions);
		});
	});
}

function getMostRecentDate() {
	return new Promise(function(resolve, reject) {
		mostRecentDate.findOne({}, function(err, date) {
			if (err) throw err;
			console.log(date);
			mongoose.disconnect();
			resolve(date);
		});
	});
}

function delete_database() {
	mongoose.connection.dropDatabase((err) => {
		if (err) throw err;
		mongoose.disconnect();
	});
}

module.exports = {upsert_transactions, get_recurring_trans, get_all_trans, getMostRecentDate, delete_database, mongoose};