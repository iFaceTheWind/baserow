import { DataSourceLocalBaserowTableServiceType } from '@baserow/modules/integrations/localBaserow/serviceTypes'
import LocalBaserowGroupedAggregateRowsForm from '@baserow_enterprise/integrations/localBaserow/components/services/LocalBaserowGroupedAggregateRowsForm'

export class LocalBaserowGroupedAggregateRowsServiceType extends DataSourceLocalBaserowTableServiceType {
  static getType() {
    return 'local_baserow_grouped_aggregate_rows'
  }

  get name() {
    return this.app.$i18n.t('serviceType.localBaserowGroupedAggregateRows')
  }

  get description() {
    return this.app.$i18n.t(
      'serviceType.localBaserowGroupedAggregateRowsDescription'
    )
  }

  get formComponent() {
    return LocalBaserowGroupedAggregateRowsForm
  }

  get icon() {
    return 'iconoir-stats-report'
  }

  get returnsList() {
    return true
  }

  getIdProperty(service, record) {
    return null
  }

  getRecordName(service, record) {
    return ''
  }

  getErrorMessage({ service }) {
    if (service !== undefined) {
      if (!service.table_id) {
        return this.app.$i18n.t('serviceType.errorNoTableSelected')
      }
      if (
        !service.aggregation_series ||
        service.aggregation_series.length === 0
      ) {
        return this.app.$i18n.t('serviceType.errorNoAggregationSeriesDefined')
      }
      const incompleteSeries = service.aggregation_series.some(
        (item) => !item.field_id || !item.aggregation_type
      )
      if (incompleteSeries) {
        return this.app.$i18n.t('serviceType.errorIncompleteAggregationSeries')
      }
      const filtersInError = service.filters?.some((filter) => filter.trashed)
      if (filtersInError) {
        return this.app.$i18n.t('serviceType.errorFilterInError')
      }
    }
    return super.getErrorMessage({ service })
  }

  beforeUpdate(newValues, oldValues) {
    if (
      oldValues.table_id !== null &&
      newValues.table_id !== oldValues.table_id
    ) {
      newValues.filters = []
      newValues.aggregation_series = []
      newValues.aggregation_group_bys = []
      newValues.aggregation_sorts = []
    }
    return newValues
  }

  getOrder() {
    return 40
  }
}
