package com.github.miemiedev.mybatis.paginator.dialect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.RowBounds;
import org.springframework.util.StringUtils;

import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;

/**
 * 类似hibernate的Dialect,但只精简出分页部分
 * 
 * @author badqiu
 * @author miemiedev
 * @author khan
 */
public class Dialect {

	public static final String WHERE_CLAUSE_PLACEHOLDER = "_WHERE_CLAUSE_PLACEHOLDER";

	protected MappedStatement mappedStatement;
	protected PageBounds pageBounds;
	protected Object parameterObject;
	protected BoundSql boundSql;
	protected List<ParameterMapping> parameterMappings;
	protected Map<String, Object> pageParameters = new HashMap<String, Object>();

	private String pageSQL;
	private String countSQL;

	public Dialect(MappedStatement mappedStatement, Object parameterObject, PageBounds pageBounds) {
		this.mappedStatement = mappedStatement;
		this.parameterObject = parameterObject;
		this.pageBounds = pageBounds;

		init();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void init() {
		boundSql = mappedStatement.getBoundSql(parameterObject);
		parameterMappings = new ArrayList<ParameterMapping>(boundSql.getParameterMappings());
		if (parameterObject instanceof Map) {
			pageParameters.putAll((Map) parameterObject);
		} else {
			for (ParameterMapping parameterMapping : parameterMappings) {
				pageParameters.put(parameterMapping.getProperty(), parameterObject);
			}
		}

		StringBuffer bufferSql = new StringBuffer(boundSql.getSql().trim());
		if (bufferSql.lastIndexOf(";") == bufferSql.length() - 1) {
			bufferSql.deleteCharAt(bufferSql.length() - 1);
		}
		String sql = bufferSql.toString();

		// 拼装WHERE 条件
		pageSQL = imbedWhereClause(sql, pageBounds.getWhereClause());
		// 拼装OrderBy条件
		pageSQL = imbedOrderClause(pageSQL, pageBounds);

		if (pageBounds.getOffset() != RowBounds.NO_ROW_OFFSET || pageBounds.getLimit() != RowBounds.NO_ROW_LIMIT) {
			pageSQL = getLimitString(pageSQL, "__offset", pageBounds.getOffset(), "__limit", pageBounds.getLimit());
		}

		countSQL = getCountString(sql);
	}

	public List<ParameterMapping> getParameterMappings() {
		return parameterMappings;
	}

	public Object getParameterObject() {
		return pageParameters;
	}

	public String getPageSQL() {
		return pageSQL;
	}

	protected void setPageParameter(String name, Object value, Class<?> type) {
		ParameterMapping parameterMapping = new ParameterMapping.Builder(mappedStatement.getConfiguration(), name, type)
				.build();
		parameterMappings.add(parameterMapping);
		pageParameters.put(name, value);
	}

	public String getCountSQL() {
		return countSQL;
	}

	/**
	 * 将sql变成分页sql语句
	 */
	protected String getLimitString(String sql, String offsetName, int offset, String limitName, int limit) {
		throw new UnsupportedOperationException("paged queries not supported");
	}

	/**
	 * 将sql转换为总记录数SQL
	 * 
	 * @param sql SQL语句
	 * @return 总记录数的sql
	 */
	protected String getCountString(String sql) {
		return "select count(1) from (" + sql + ") tmp_count";
	}

	/**
	 * 在原sql中简单置入where条件<br/>
	 * 原则如下：
	 * <ul>
	 * <li>如果 sql中使用 WHERE_CLAUSE_PLACEHOLDER 占位符 则直接替换</li>
	 * <li>如果存在where，则直接替换第一个</li>
	 * <li>没使用占位符且不存在where的，直接添加在最后</li>
	 * </ul>
	 * 
	 * @param sql 原始sql
	 * @param clause 不含“where”的搜索条件
	 * @return
	 */
	protected String imbedWhereClause(String sql, String clause) {
		if (!StringUtils.hasText(clause)) {
			return sql;
		}
		boolean containsWhere = sql.matches("[\\s\\S]* (?i)where [\\s\\S]*");
		if (!containsWhere) {
			clause = " where ".concat(clause);
		}
		if (sql.contains(WHERE_CLAUSE_PLACEHOLDER)) {
			// 如果 sql中使用 WHERE_CLAUSE_PLACEHOLDER 占位符 则直接替换
			return sql.replace(WHERE_CLAUSE_PLACEHOLDER, clause);
		}
		if (containsWhere) {
			// 如果存在where，则直接替换第一个
			return sql.replaceFirst(" (?i)where ", clause).concat(" and ");
		}
		// 没使用占位符且不存在where的，直接添加在最后
		return sql.concat(clause);
	}

	/**
	 * 在原sql中简单置入OrderBy条件<br/>
	 * 
	 * @param sql
	 * @param pageBounds
	 * @return
	 */
	protected String imbedOrderClause(String sql, PageBounds pageBounds) {
		if (!pageBounds.hasOrderByClause()) {
			return sql;
		}
		StringBuilder sqlBuilder = new StringBuilder("select * from (").append(sql).append(") temp_order order by ");
		if (StringUtils.hasText(pageBounds.getOrderByClause())) {
			return sqlBuilder.append(pageBounds.getOrderByClause()).toString();
		}
		for (Order order : pageBounds.getOrders()) {
			if (order != null) {
				sqlBuilder.append(order.toString()).append(", ");
			}

		}
		sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
		return sqlBuilder.toString();
	}

}
