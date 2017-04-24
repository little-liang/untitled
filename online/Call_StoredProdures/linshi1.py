aa = \
    {
        'task_group_id':
            {
                '1':
                    {
                        'call_type':
                            {
                                '1':
                                    {
                                        'run_time': ['06:00', '09:00'],
                                    },

                                '3':
                                    {
                                        'run_time': ['10:00'],
                                    },

                                '2':
                                    {
                                        'run_time': ['10:00'],
                                    },
                            }
                    },
                '2':
                    {
                        'call_type':
                            {
                                '1':
                                    {
                                        'run_time': ['08:00', '07:00', '09:00']
                                    },
                                '2':
                                    {
                                        'run_time': ['07:00']
                                    }
                            }
                    }
            }
    }

bb = \
    {
        'task_group_id':
            {
                '1':
                    {
                        'other_prejob': 'None',
                        'job_id':
                        {
                            '3':
                                {
                                    'return_info': 'o_return_code,o_return_message',
                                    'storeprodure_name': 'WSD.PACK_INV_ACTUAL_VALUE.PROC_ALL',
                                    'prejob_id': ['2'], 'para_info': 'i_data_date'},
                            '1':
                                {
                                    'return_info': 'o_return_code,o_return_message',
                                    'storeprodure_name': 'WSD.PACK_INV_ACTUAL_VALUE.PROC_ALL',
                                    'prejob_id': ['None'],
                                    'para_info': 'i_data_date'
                                },
                            '2':
                                {
                                    'return_info': 'o_return_code,o_return_message',
                                    'storeprodure_name': 'WSD.pack_fin_income_margin.proc_all',
                                    'prejob_id': ['1'], 'para_info': 'i_data_date'
                                },
                            '4':
                                {'return_info': 'o_return_code,o_return_message',
                                 'storeprodure_name': 'DW.PACK_YANLONG_TEST_2.proc_a',
                                 'prejob_id': ['None'],
                                 'para_info': 'i_data_date'
                                 }
                        }
                    },
                '2':
                    {
                        'other_prejob': 'None',
                        'job_id':
                            {
                                '1':
                                    {
                                        'return_info': 'o_return_code,o_return_message',
                                        'storeprodure_name': 'WSD.pack_fin_income_margin.proc_all',
                                        'prejob_id': ['None'], 'para_info': 'i_data_date'
                                    },
                                '2': {'return_info': 'o_return_code,o_return_message',
                                      'storeprodure_name': 'WSD.PACK_INV_ACTUAL_VALUE.PROC_ALL',
                                      'prejob_id': ['1'], 'para_info': 'i_data_date'
                                      }
                            }
                    }
            }
    }
