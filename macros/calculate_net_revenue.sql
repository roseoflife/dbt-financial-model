{% macro calculate_net_revenue(trans_amount, refund_amount) %}
  SUM({{ trans_amount }} - COALESCE({{ refund_amount }}, 0)) AS net_revenue
{% endmacro %}