describe('Enterprise integrations service types', () => {
  test('LocalBaserowGroupedAggregateRowsServiceType is registered as a builder data source', () => {
    const testApp = useNuxtApp()
    const serviceType = testApp.$registry.get(
      'service',
      'local_baserow_grouped_aggregate_rows'
    )

    expect(serviceType).toBeDefined()
    expect(serviceType.isDataSource).toBe(true)
    expect(serviceType.returnsList).toBe(true)
    expect(serviceType.formComponent).toBeDefined()
    expect(serviceType.integrationType.getType()).toBe('local_baserow')
  })

  test('LocalBaserowGroupedAggregateRowsServiceType reports an error when no series defined', () => {
    const testApp = useNuxtApp()
    const serviceType = testApp.$registry.get(
      'service',
      'local_baserow_grouped_aggregate_rows'
    )

    const error = serviceType.getErrorMessage({
      service: {
        table_id: 1,
        aggregation_series: [],
        filters: [],
      },
    })
    expect(error).toBeTruthy()
  })

  test('LocalBaserowGroupedAggregateRowsServiceType reports an error when a series is incomplete', () => {
    const testApp = useNuxtApp()
    const serviceType = testApp.$registry.get(
      'service',
      'local_baserow_grouped_aggregate_rows'
    )

    const error = serviceType.getErrorMessage({
      service: {
        table_id: 1,
        aggregation_series: [{ field_id: null, aggregation_type: 'sum' }],
        filters: [],
      },
    })
    expect(error).toBeTruthy()
  })

  test('LocalBaserowGroupedAggregateRowsServiceType resets configuration on table change', () => {
    const testApp = useNuxtApp()
    const serviceType = testApp.$registry.get(
      'service',
      'local_baserow_grouped_aggregate_rows'
    )

    const newValues = serviceType.beforeUpdate(
      {
        table_id: 2,
        filters: [{ id: 1 }],
        aggregation_series: [{ field_id: 1, aggregation_type: 'sum' }],
        aggregation_group_bys: [{ field_id: 2 }],
        aggregation_sorts: [{ reference: 'field_1_sum', direction: 'ASC' }],
      },
      { table_id: 1 }
    )
    expect(newValues.filters).toEqual([])
    expect(newValues.aggregation_series).toEqual([])
    expect(newValues.aggregation_group_bys).toEqual([])
    expect(newValues.aggregation_sorts).toEqual([])
  })
})
