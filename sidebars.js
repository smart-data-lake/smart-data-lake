module.exports = {
  docs: [
    {'Smart Data Lake' : [
      'intro',
      'features',
      'architecture'
    ]},
    {'Getting Started' : [
      'getting-started/setup',
      'getting-started/get-input-data',
      {
        'Part 1': [
          'getting-started/part-1/get-departures',
          'getting-started/part-1/get-airports',
          'getting-started/part-1/select-columns',
          'getting-started/part-1/joining-it-together',
          'getting-started/part-1/joining-departures-and-arrivals',
          'getting-started/part-1/compute-distances'
        ],
        'Part 2': [
            'getting-started/part-2/industrializing',
            'getting-started/part-2/delta-lake-format',
            'getting-started/part-2/historical-data'
        ],
        'Part 3': [
          'getting-started/part-3/custom-webservice',
          'getting-started/part-3/incremental-mode'
        ],
        'Troubleshooting': [
          'getting-started/troubleshooting/common-problems',
          'getting-started/troubleshooting/docker-on-windows'
        ]
      }

    ]},
    {'Reference' : [
        'reference/build',
        'reference/commandLine',
        {'SDLB objects' : [
          'reference/dataObjects',
          'reference/actions',
        ]},
        {'Hocon Configuration' : [
          'reference/hoconOverview',
          'reference/hoconVariables',
          'reference/hoconSecrets',
        ]},
        'reference/dag',
        'reference/schema',
        'reference/dataQuality',
        'reference/executionPhases',
        'reference/executionEngines',
        'reference/executionModes',
        'reference/transformations',
        //'reference/schemaEvolution',
        //'reference/housekeeping',
        'reference/streaming',
        {'Deployment' : [
          'reference/deploymentOptions',
          'reference/deploy-microsoft-azure',
        ]},
        'reference/testing',
        'reference/troubleshooting',
        //'reference/glossary'
    ]},
    {
      type: 'link',
      label: 'Configuration Schema Viewer', // The link label
      href: '/json-schema-viewer', // The internal path
    },
  ],
};
