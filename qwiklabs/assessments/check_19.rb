<<<<<<< HEAD

def check_19(handles:, maximum_score:, resources:)
  # Service handle initialization
  logging = handles['project_0.LoggingV2']

  # Assessment score and status checker hash variable
  ret_hash = { :score => 0, :message => "", :student_message => "" }

  lab_access_time_in_sec = 54000
  lab_start_time = Time.at(Time.now.to_i - lab_access_time_in_sec)

  log_filters =[
    'timestamp>"' + lab_start_time.utc.iso8601 + '"',
    'protoPayload.serviceName="pubsub.googleapis.com"',
    'protoPayload.methodName="google.iam.v1.IAMPolicy.SetIamPolicy"',
    'resource.labels.topic_id="projects/'+logging.project+'/topics/recapture_customer"',
    'severity="NOTICE"'
  ]

  # Build log filter
  custom_filter = log_filters.join(" AND ")

  # List_log_entries_request_object
  list_log_entries_request_object = Google::Apis::LoggingV2::ListLogEntriesRequest.new

  # Parent resource to track
  list_log_entries_request_object.resource_names = ['projects/' + logging.project]

  list_log_entries_request_object.filter = custom_filter
  query_logs = logging.list_entry_log_entries(list_log_entries_request_object)&.entries || []

  # Validate if logs > 0 found
  if query_logs.count > 0
    ret_hash = { :score => maximum_score, :message => '성공적으로 권한이 갱신되었습니다.', :student_message => '성공적으로 권한이 갱신되었습니다.' }
  else
    error_message = "다시 한 번 절차를 확인해보세요!"
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
 
  return ret_hash
end