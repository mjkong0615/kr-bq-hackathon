# method description
def check_10(handles:, maximum_score:, resources:)
  bigquery = handles['project_0.BigqueryV2']
  ret_hash = { :score => 0, :message => "", :student_message => "" }
  isAvailable = false

  begin
    negative_customers_list_object = bigquery.get_table("cymbal".to_s, "negative_customers_list".to_s ) || []
    negative_customer_segment_data_object = bigquery.get_table("cymbal".to_s, "negative_customer_segment_data".to_s ) || []
  rescue 
    isAvailable = false
  end 

  if negative_customers_list_object && negative_customer_segment_data_object
    isAvailable = true
  end 

  if isAvailable 
    success_message = "테이블이 정상적으로 생성되었습니다."
    ret_hash = { :done => true, :score => maximum_score, :message => success_message, :student_message => success_message}
  else
    error_message = "다시 한 번 절차를 확인해보세요!"
    ret_hash[:message] = error_message
    ret_hash[:student_message] = error_message
  end
  return ret_hash
end