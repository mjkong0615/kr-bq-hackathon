# method description
def check_06(handles:, maximum_score:, resources:)
  bigquery = handles['project_0.LoggingV2']
  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = true

   # last task filter
  log_filters =[
    'protoPayload.serviceName="bigquery.googleapis.com"',
    'protoPayload.serviceData.jobGetQueryResultsResponse.job.jobStatistics.referencedTables.tableId="multimodal_customer_reviews"',
    'protoPayload.serviceData.jobGetQueryResultsResponse.job.jobConfiguration.query.statementType="SELECT"'
  ]

  # Build log filter
  custom_filter = log_filters.join(" AND ")

  # List_log_entries_request_object
  list_log_entries_request_object = Google::Apis::LoggingV2::ListLogEntriesRequest.new

  # Parent resource to track
  list_log_entries_request_object.resource_names = ['projects/' + logging.project]

  list_log_entries_request_object.filter = custom_filter
  query_logs = logging.list_entry_log_entries(list_log_entries_request_object)&.entries || []

  if isAvailable 
    ret_hash = { :done => true, :score => maximum_score, :message => "Assessment completed!", :student_message => "All Hands-on Step completed!"}
  else
    error_message = 'Please try all the Hands-on steps again.'
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
  return ret_hash
end